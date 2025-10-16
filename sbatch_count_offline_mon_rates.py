#!/usr/bin/env python3
"""
Submit a SLURM array job to add environment data and count offline beta monitor rates for many run_ids.

Usage:
  sbatch_count_offline_mon_rates.py -rids 1748 1749 ... -aid 13
"""

import subprocess as sp
import argparse
from typing import List
from pathlib import Path


def main():

    par = argparse.ArgumentParser()
    arg, st = par.add_argument, "store_true"

    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--runids", nargs="+", type=int, help="run ids to analyze")
    arg("-aid", "--analysis_id", type=int, default=-1, help="analysis_id")

    args = par.parse_args()

    tlim = "10:00:00" if args.tlim is None else args.tlim[0]
    analysis_id = args.analysis_id

    # Command to run inside the container
    # Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.

    apptainer_prefix = (
        "\"apptainer exec --bind /data/raid2/eliza4/he6_cres/ "
        "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
        "/bin/bash -c $'umask 002; source /data/raid2/eliza4/he6_cres/.bashrc {} "
    ).format(r"\n")

    for run_id in args.runids:
        count_cmd = (
                f"/opt/python3.7/bin/python3.7 -u /data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/count_offline_mon_rates.py "
            f"-rid {run_id} -aid {analysis_id}"
        )
        cmd = apptainer_prefix + f"{count_cmd}'\""
        sbatch_job(run_id, analysis_id, cmd, tlim)


def sbatch_job(run_id, analysis_id, cmd, tlim):
    """
    Replaces SGE qsub with Slurm sbatch.
    Uses --wrap for inline command submission.
    """
    log_path = f"/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/katydid/rid_{run_id:04d}_{analysis_id:03d}_count_offline_mon.txt"

    sbatch_opts = [
        "--job-name", f"r{run_id}_a{analysis_id}_count_offline_mon",
        "--time", tlim,
        "--output", log_path,
        "--export=ALL",
        "--mail-type=NONE",
    ]

    sbatch_str = " ".join(sbatch_opts)
    batch_cmd = f"sbatch {sbatch_str} --wrap={cmd}"

    print("\n\n", batch_cmd, "\n\n")
    sp.run(batch_cmd, shell=True)

if __name__ == "__main__":
    main()