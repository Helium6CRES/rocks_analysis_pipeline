#!/usr/bin/env python3
"""
Helper script to add environment data and compute offline beta monitor event counts for a single run_id.

Each compute node should run this script for one run_id in parallel.
It uses the same PostProcessing class from run_post_processing_2025LTF.py.
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from run_post_processing_2025LTF import PostProcessing

def count_offline_mon_for_run(run_id: int, analysis_id: int, ms_standard: int = 1):
    """
    Loads the per-run CSV, adds offline monitor event counts, and writes an updated CSV.
    """
    print(f"\n[{datetime.now()}] Starting offline monitor count for run_id={run_id}, analysis_id={analysis_id}\n")

    # Instantiate PostProcessing for this run_id (no multi-run or heavy setup)
    pp = PostProcessing(
        run_ids=[run_id],
        analysis_id=analysis_id,
        experiment_name=None,
        num_files_tracks=0,
        file_id=0,
        stage=-1,  # just using its utilities
        ms_standard=ms_standard,
    )

    # Path to per-run root_files CSV
    file_df_path = pp.build_file_df_path(run_id)
    if not file_df_path.exists():
        raise FileNotFoundError(f"Missing file_df for run_id {run_id}: {file_df_path}")

    print(f"Loading {file_df_path}")
    file_df = pd.read_csv(file_df_path)

    # Compute offline monitor counts
    updated_df = pp.add_env_data(file_df)

    # Compute offline monitor counts
    updated_df = pp.add_offline_monitor_counts(updated_df)

    # Write to new CSV (avoid overwriting original)
    out_path = file_df_path.with_name(file_df_path.stem + "_with_offline_mon.csv")
    updated_df.to_csv(out_path, index=False)
    print(f"Wrote updated file: {out_path}")

    print(f"\n[{datetime.now()}] Finished offline monitor count for run_id={run_id}\n")

    return out_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Compute offline beta monitor event counts for a single run_id."
    )
    parser.add_argument("-rid", "--run_id", type=int, required=True, help="Run ID to process.")
    parser.add_argument("-aid", "--analysis_id", type=int, required=True, help="Analysis ID.")
    parser.add_argument(
        "-ms",
        "--ms_standard",
        type=int,
        default=1,
        help="0 = filename to second precision, 1 = filename includes milliseconds (default=1).",
    )

    args = parser.parse_args()
    count_offline_mon_for_run(args.run_id, args.analysis_id, ms_standard=args.ms_standard)
