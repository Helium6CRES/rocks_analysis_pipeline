import os
import re
import pytz
import datetime
import numpy as np
import pandas as pd
from pathlib import Path

# Local imports.
from rocks_utility import (
    he6cres_db_query,
    get_pst_time,
    set_permissions,
    check_if_exists,
    log_file_break,
)

# ----------------------------------------------------------------------
# Assumed available: he6cres_db_query(sql_query: str) -> pandas.DataFrame
# ----------------------------------------------------------------------

# Map gases in the order written by the RGA log
GASES = [
    "nitrogen", "helium", "co2", "hydrogen",
    "water", "oxygen", "krypton", "argon",
    "cf3", "a19"
]

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# ----------------------------------------------------------------------
# Helper to parse file creation timestamp from RGA header
# ----------------------------------------------------------------------
def parse_file_start_time(rga_path: str) -> datetime.datetime:
    """Robustly find 'Start time, Mon DD, YYYY  HH:MM:SS AM/PM' and return UTC-aware datetime."""
    with open(rga_path, "r") as f:
        lines = f.readlines()

    # Don’t depend on a fixed line number; search for the header line.
    pat = re.compile(r"^Start time,\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}:\d{2}:\d{2})\s+(AM|PM)")
    for line in lines:
        m = pat.match(line.strip())
        if m:
            mon_txt, day, year, hms, ampm = m.groups()
            month = MONTHS[mon_txt]
            day   = int(day)
            year  = int(year)
            hh, mm, ss = map(int, hms.split(":"))
            if ampm == "PM" and hh != 12:
                hh += 12
            if ampm == "AM" and hh == 12:
                hh = 0
            local = datetime.datetime(year, month, day, hh, mm, ss)
            pacific = pytz.timezone("US/Pacific")
            return pacific.localize(local).astimezone(pytz.utc)

    raise RuntimeError("Could not find 'Start time' line in RGA file.")



# ----------------------------------------------------------------------
# Helper to parse all measurement lines and compute UTC timestamps
# ----------------------------------------------------------------------
def read_rga_log(rga_path: str) -> pd.DataFrame:
    """Parse all measurement lines into UTC timestamps + pressures."""
    with open(rga_path, "r") as f:
        lines = f.readlines()

    file_start_utc = parse_file_start_time(rga_path)
    t0 = file_start_utc.timestamp()

    rows = []
    for line in lines:
        # Data lines start with a float (seconds since file creation), then a comma
        if not re.match(r"^\d", line):
            continue
        parts = line.split(",", 1)
        if len(parts) != 2:
            continue
        try:
            rel_s = float(parts[0])  # seconds since file creation
        except ValueError:
            continue
        utc_ts = datetime.datetime.fromtimestamp(t0 + rel_s, tz=datetime.timezone.utc)

        pressure_line = parts[1]
        vals = []
        i = 0
        while i < len(pressure_line):
            ch = pressure_line[i]
            if ch in (" ", "\n", "\r", "\t"):
                i += 1
                continue
            if ch == "-":
                # negative mantissa → 12 chars like '-1.23e-09'
                try:
                    vals.append(float(pressure_line[i:i+12]))
                except ValueError:
                    break
                i += 12
            elif ch.isdigit():
                # positive mantissa → 11 chars like '1.23e-09'
                try:
                    vals.append(float(pressure_line[i:i+11]))
                except ValueError:
                    break
                i += 11
            else:
                # unexpected char; give up on this line
                break

        if len(vals) == len(GASES):
            row = {"utc_time": utc_ts}
            row.update({g: v for g, v in zip(GASES, vals)})
            rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No data lines parsed from RGA file.")
    df["total"] = df[GASES].where(df[GASES] > 0, 0.0).sum(axis=1)
    return df

# ----------------------------------------------------------------------
# Main check function
# ----------------------------------------------------------------------
def check_rga_vs_sql(rga_path: str,
                     time_tolerance_s: float = 5.0,
                     pressure_atol: float = 5e-13,
                     allow_constant_offset_alignment: bool = True,
                     constant_offset_tolerance_s: float = 5.0):
    """
    Compare RGA text log vs SQL table.
    If a near-constant timestamp offset exists (e.g., DAQ kept old fileUTCstamp),
    optionally align by the median offset and still compare gas values.
    """
    print(f"Checking RGA log: {rga_path}")
    df_log = read_rga_log(rga_path).sort_values("utc_time")
    tmin_aware = df_log["utc_time"].min().floor("min")
    tmax_aware = df_log["utc_time"].max().ceil("min")

    # DB stores naive UTC → drop tz for the query bounds
    tmin = pd.Timestamp(tmin_aware).tz_localize(None)
    tmax = pd.Timestamp(tmax_aware).tz_localize(None)

    query = f"""
        SELECT created_at, nitrogen, helium, co2, hydrogen,
               water, oxygen, krypton, argon, cf3, a19, total
        FROM he6cres_runs.rga
        WHERE created_at BETWEEN '{tmin}'::timestamp AND '{tmax}'::timestamp
    """
    df_db = he6cres_db_query(query)
    if df_db.empty:
        print("No DB rows in this window.")
        return df_log, df_db, [], []

    # Make tz-aware UTC for comparison
    df_db["created_at"] = pd.to_datetime(df_db["created_at"]).dt.tz_localize("UTC")
    df_db = df_db.sort_values("created_at")

    # For each log time, find nearest DB time, collect deltas
    def nearest_idx(t):
        return (df_db["created_at"] - t).abs().argmin()

    deltas = []
    nearest_ix = []
    for t in df_log["utc_time"]:
        ix = nearest_idx(t)
        nearest_ix.append(ix)
        deltas.append((df_db.iloc[ix]["created_at"] - t).total_seconds())

    deltas = np.array(deltas)
    median_offset = float(np.median(deltas))
    mad = float(np.median(np.abs(deltas - median_offset)))

    print(f"\nTime alignment diagnostics:")
    print(f"  log  window: {tmin_aware}  →  {tmax_aware}")
    print(f"  DB   window: {df_db['created_at'].min()}  →  {df_db['created_at'].max()}")
    print(f"  median Δt (DB - log): {median_offset:.2f} s   (MAD ≈ {mad:.2f} s)")

    mismatches = []
    unmatched = []

    # Decide whether to allow alignment by median offset
    align = allow_constant_offset_alignment and (abs(median_offset) > time_tolerance_s)

    for i, row in df_log.iterrows():
        t_log = row["utc_time"]
        ix = nearest_ix[i - df_log.index[0]]  # map back into df_db
        t_db = df_db.iloc[ix]["created_at"]
        dt = (t_db - t_log).total_seconds()

        # If not aligning and too far → timestamp mismatch
        if not align and abs(dt) > time_tolerance_s:
            mismatches.append((t_log, "timestamp", dt))
            continue

        # If aligning, require dt to be close to the median offset
        if align and abs(dt - median_offset) > constant_offset_tolerance_s:
            mismatches.append((t_log, "timestamp", dt))
            continue

        # Compare gas values even when we aligned by offset
        for gas in GASES + ["total"]:
            v_log = row[gas]
            v_db = df_db.iloc[ix][gas]
            # Treat NaNs as mismatches only if exactly one side is NaN
            if (pd.isna(v_log) and pd.isna(v_db)):
                continue
            if not np.isclose(v_log, v_db, atol=pressure_atol, equal_nan=False):
                mismatches.append((t_log, gas, v_log - v_db))

    # Summary
    print(f"\nChecked {len(df_log)} log entries.")
    if align:
        print(f"Aligned by median offset of {median_offset:.2f} s (±{constant_offset_tolerance_s}s).")
    print(f"Mismatches reported: {len(mismatches)}\n")

    # Preview some mismatches
    if mismatches:
        print("Mismatches (up to 10):")
        for t, field, diff in mismatches[:10]:
            print(f"  {t}  {field:10s}  Δ={diff}")

    return df_log, df_db, mismatches, unmatched



# ----------------------------------------------------------------------
# Example usage
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Replace this with your actual file path
    test_path = "/media/heather/T7/RGA_data/072725-head1.rga_Jul_28_2025_11-48-16_AM.txt"
    check_rga_vs_sql(test_path)
