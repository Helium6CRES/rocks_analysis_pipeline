#!/usr/bin/env python3
import subprocess as sp
import argparse
from typing import List
from pathlib import Path


def main():
    """
    Slurm submission script for Katydid analysis.
    """
    par = argparse.ArgumentParser()
    arg, st = par.add_argument, "store_true"

    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--runids", nargs="+", type=int, help="run ids to analyze")
    arg("-nid", "--noise_run_id", type=int, help="run_id to use for noise floor in katydid run.")
    arg("-b", "--base_config", type=str, help="base .yaml katydid config file to be run on run_id.")
    arg("-fn", "--file_num", default=-1, type=int, help="Number of files in run id to analyze.")
    arg("-aid", "--analysis_id", type=int, default=-1, help="analysis_id used to label directories. If -1, a new index will be created.")

    args = par.parse_args()

    tlim = "48:00:00" if args.tlim is None else args.tlim[0]

    # If the analysis_id is set to -1 then a new directory is built.
    # Else you will conduct a clean-up.
    if args.analysis_id == -1:
        # Get the analysis index to use for the list of jobs.
        analysis_id = get_analysis_id(args.runids)
        print(f"analysis_id: {analysis_id}")
    else:
        analysis_id = args.analysis_id

    # Command to run inside the container
    # Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.

    apptainer_prefix = (
        "\"apptainer exec --bind /data/raid2/eliza4/he6_cres/ "
        "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
        "/bin/bash -c $'umask 002; source /data/raid2/eliza4/he6_cres/.bashrc {} "
    ).format(r"\n")

    for run_id in args.runids:
        default_katydid_sub = (
                f"/opt/python3.7/bin/python3.7 -u /data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_katydid.py "
            f"-id {run_id} -nid {args.noise_run_id} -aid {analysis_id} -b \"{args.base_config}\" -fn {args.file_num} "
        )
        cmd = apptainer_prefix + f"{default_katydid_sub}'\""
        sbatch_job(run_id, analysis_id, cmd, tlim)


def sbatch_job(run_id, analysis_id, cmd, tlim):
    """
    Replaces SGE qsub with Slurm sbatch.
    Uses --wrap for inline command submission.
    """
    log_path = f"/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/katydid/rid_{run_id:04d}_{analysis_id:03d}.txt"

    sbatch_opts = [
        "--job-name", f"r{run_id}_a{analysis_id}",
        "--time", tlim,
        "--output", log_path,
        "--export=ALL",
        "--mail-type=NONE",
    ]

    sbatch_str = " ".join(sbatch_opts)
    batch_cmd = f"sbatch {sbatch_str} --wrap={cmd}"

    print("\n\n", batch_cmd, "\n\n")
    sp.run(batch_cmd, shell=True)


def get_analysis_id(run_ids):
    """
    We want each analysis run simultaneously to have the same analysis number.
    This function goes through and builds the directory structure out and
    checks to see what the next possible analysis index is such that all run_ids
    recieve the same analysis index.

    """
    base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/root_files")
    max_analysis_ids = []

    for run_id in run_ids:
        run_id_dir = base_path / f"rid_{run_id:04d}"
        if not run_id_dir.is_dir():
            run_id_dir.mkdir()
            print(f"Created directory: {run_id_dir}")
        
        # Robust against deleted or missing aids.
        analysis_ids = [
            int(f.name[-3:]) for f in run_id_dir.iterdir() if f.is_dir()
        ]
        print(f"run_id = {run_id}. Existing aids = {sorted(analysis_ids)}")
        # Use the fact that an empty list is boolean False.
        max_analysis_ids.append(max(analysis_ids) if analysis_ids else 0)

    return max(max_analysis_ids) + 1


if __name__ == "__main__":
    main()
