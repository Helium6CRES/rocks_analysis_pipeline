#!/usr/bin/env python3

'''
usage:
python count_tracks_in_roots.py /data/raid2/eliza4/he6_cres/katydid_analysis/saved_experiments/YOUR_EXPERIMENT_NAME_aid_0/root_files.csv
'''
import argparse
from pathlib import Path

import pandas as pd
import uproot


def count_tree_entries(root_path):
    try:
        with uproot.open(root_path) as f:
            keys = set(f.keys())

            if "MB-events;1" in keys:
                tree = f["MB-events;1"]
                return {
                    "tree_type": "MB-events",
                    "entries": tree.num_entries,
                    "keys": ",".join(f.keys()),
                    "error": "",
                }

            if "tracks;1" in keys:
                tree = f["tracks;1"]
                return {
                    "tree_type": "tracks",
                    "entries": tree.num_entries,
                    "keys": ",".join(f.keys()),
                    "error": "",
                }

            if "multiTrackEvents;1" in keys:
                tree = f["multiTrackEvents;1"]
                return {
                    "tree_type": "multiTrackEvents",
                    "entries": tree.num_entries,
                    "keys": ",".join(f.keys()),
                    "error": "",
                }

            return {
                "tree_type": "NO_RECOGNIZED_TRACK_TREE",
                "entries": 0,
                "keys": ",".join(f.keys()),
                "error": "",
            }

    except Exception as exc:
        return {
            "tree_type": "ERROR",
            "entries": 0,
            "keys": "",
            "error": repr(exc),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root_files_csv")
    parser.add_argument("--max-files", type=int, default=None)
    args = parser.parse_args()

    df = pd.read_csv(args.root_files_csv)

    if "root_file_exists" in df.columns:
        df = df[df["root_file_exists"].astype(str).str.lower().isin(["true", "1"])]

    if args.max_files is not None:
        df = df.head(args.max_files)

    rows = []
    n_total = len(df)

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        root_path = row["root_file_path"]
        info = count_tree_entries(root_path)

        out = {
            "run_id": row.get("run_id"),
            "file_id": row.get("file_id"),
            "pp_file_id": row.get("pp_file_id"),
            "analysis_variant": row.get("analysis_variant"),
            "set_field": row.get("set_field", row.get("fake_field", row.get("field"))),
            "root_file_path": root_path,
        }
        out.update(info)
        rows.append(out)

        print(
            f"[{i:5d}/{n_total:5d}] "
            f"rid={out['run_id']} "
            f"fid={out['file_id']} "
            f"ppid={out['pp_file_id']} "
            f"field={out['set_field']} "
            f"tree={out['tree_type']} "
            f"entries={out['entries']} "
            f"path={root_path}",
            flush=True,
        )

        if out["error"]:
            print(f"    ERROR: {out['error']}", flush=True)

    out_df = pd.DataFrame(rows)

    print("\nSummary by tree type:")
    print(
        out_df.groupby("tree_type", dropna=False)
        .agg(
            n_files=("root_file_path", "size"),
            total_entries=("entries", "sum"),
            files_with_entries=("entries", lambda x: (x > 0).sum()),
        )
        .reset_index()
        .to_string(index=False)
    )

    if "set_field" in out_df.columns:
        print("\nSummary by field and tree type:")
        print(
            out_df.groupby(["set_field", "tree_type"], dropna=False)
            .agg(
                n_files=("root_file_path", "size"),
                total_entries=("entries", "sum"),
                files_with_entries=("entries", lambda x: (x > 0).sum()),
            )
            .reset_index()
            .sort_values(["set_field", "tree_type"])
            .to_string(index=False)
        )

    bad = out_df[(out_df["tree_type"] == "ERROR") | (out_df["tree_type"] == "NO_RECOGNIZED_TRACK_TREE")]
    if len(bad):
        print("\nFiles with errors or no recognized track tree:")
        cols = [
            "run_id",
            "file_id",
            "pp_file_id",
            "analysis_variant",
            "set_field",
            "tree_type",
            "error",
            "keys",
            "root_file_path",
        ]
        print(bad[cols].to_string(index=False))

    out_path = Path("root_track_entry_summary.csv")
    out_df.to_csv(out_path, index=False)
    print(f"\nWrote detailed summary to: {out_path}")


if __name__ == "__main__":
    main()