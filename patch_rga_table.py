#!/usr/bin/env python3
import re
import pytz
import numpy as np
import pandas as pd
import datetime
from pathlib import Path
import sys

# Local imports.
from rocks_utility import (
    he6cres_db_query,
    get_pst_time,
    set_permissions,
    check_if_exists,
    log_file_break,
)
sys.path.append("/home/heather/He6DAQ")
from Control_Logic import PostgreSQL_Interface as he6db

# ---------------- Configuration ----------------
RGA_FILE = "/media/heather/T7/RGA_data/072725-head1.rga_Jul_28_2025_11-48-16_AM.txt"
PATCH_END_UTC = pd.Timestamp("2025-07-29 12:34:23.6", tz=datetime.timezone.utc)
FIRST_GOOD_CREATED_AT = pd.Timestamp("2025-07-29 12:34:23.6", tz=datetime.timezone.utc)
GASES = ["nitrogen","helium","co2","hydrogen","water","oxygen","krypton","argon","cf3","a19","total"]
MONTHS = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
          "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
# ----------------------

def parse_file_start_time(path: str) -> datetime.datetime:
    with open(path) as f:
        for line in f:
            m = re.match(r"^Start time,\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}:\d{2}:\d{2})\s+(AM|PM)", line.strip())
            if m:
                mon, day, year, hms, ampm = m.groups()
                hh, mm, ss = map(int, hms.split(":"))
                if ampm == "PM" and hh != 12: hh += 12
                if ampm == "AM" and hh == 12: hh = 0
                local = datetime.datetime(int(year), MONTHS[mon], int(day), hh, mm, ss)
                pac = pytz.timezone("US/Pacific")
                return pac.localize(local).astimezone(pytz.utc)
    raise RuntimeError("No Start time line found")

def read_rga_log(path: str) -> pd.DataFrame:
    t0 = parse_file_start_time(path).timestamp()
    rows = []
    with open(path) as f:
        for line in f:
            if not re.match(r"^\d", line): continue
            parts = line.split(",",1)
            try: rel=float(parts[0])
            except: continue
            utc = datetime.datetime.fromtimestamp(t0+rel, tz=datetime.timezone.utc)
            s, i, vals = parts[1], 0, []
            while i < len(s):
                ch=s[i]
                if ch in (" ","\n","\r","\t"): i+=1; continue
                try:
                    w=12 if ch=="-" else 11
                    vals.append(float(s[i:i+w])); i+=w
                except: break
            if len(vals)==len(GASES)-1:
                total=sum(v for v in vals if v>0); vals.append(total)
            if len(vals)==len(GASES):
                rows.append([utc]+vals)
    df=pd.DataFrame(rows,columns=["utc_time"]+GASES)
    return df.sort_values("utc_time")

def main():
    df_log=read_rga_log(RGA_FILE)
    print(f"RGA log: {df_log['utc_time'].min()} → {df_log['utc_time'].max()}  ({len(df_log)} pts)")

    # Fetch all bad DB rows (zeros) before first good created_at
    q=f"""
        SELECT rga_id, created_at, utc_write_time, time_since_write, {', '.join(GASES)}
        FROM he6cres_runs.rga
        WHERE created_at < '{FIRST_GOOD_CREATED_AT.tz_convert(None)}'
          AND ({' OR '.join([f'{g}=0' for g in GASES[:-1]])})
        ORDER BY created_at;
    """
    df_db=he6cres_db_query(q)
    if df_db.empty:
        print("No zero rows before first good entry.")
        return
    df_db["created_at"]=pd.to_datetime(df_db["created_at"]).dt.tz_localize("UTC")
    print(f"Found {len(df_db)} rows to patch.")

    # Match by created_at to latest RGA log value
    df_m = pd.merge_asof(
        df_db.sort_values("created_at"),
        df_log.sort_values("utc_time"),
        left_on="created_at", right_on="utc_time",
        direction="backward", tolerance=pd.Timedelta("1H")
    )

    updates=[]
    for _,r in df_m.iterrows():
        if pd.isna(r["utc_time"]): continue
        dt=round((r["created_at"]-r["utc_time"]).total_seconds(),3)
        set_parts=[f"{g}={r[g+'_y']}" for g in GASES]
        set_parts += [f"utc_write_time='{r['utc_time'].tz_convert(None)}'",
                      f"time_since_write={dt}"]
        updates.append(f"UPDATE he6cres_runs.rga SET {', '.join(set_parts)} WHERE rga_id={int(r['rga_id'])};")

    Path("rga_patch_by_created_at.sql").write_text("\n".join(updates))
    print(f"Wrote {len(updates)} UPDATEs → rga_patch_by_created_at.sql")

    if input("Execute now? [y/N] ").strip().lower()=="y":
        conn=he6db.he6cres_db_connection(); cur=conn.cursor()
        for cmd in updates: cur.execute(cmd)
        conn.commit(); cur.close(); conn.close()
        print("✅  Database patched successfully.")
    else:
        print("Review the SQL file before applying.")

if __name__=="__main__":
    main()
