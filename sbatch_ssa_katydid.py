#!/usr/bin/env python3
import subprocess as sp
import argparse
from typing import List
from pathlib import Path
import re

def main():
    """
    Slurm submission script for spec-sims Katydid analysis.
    """
    par = argparse.ArgumentParser()
    arg, st = par.add_argument, "store_true"

    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg("-r", "--run_names", nargs="+", type=str, help="run names to analyze")
    arg("-nid", "--noise_run_id", type=int, help="run_id to use for noise floor in katydid run.")
    arg("-b", "--base_config", type=str, help="base .yaml katydid config file to be run on run_id.")
    arg("-aid", "--analysis_id", type=int, default=-1, help="analysis_id used to label directories. If -1, creates unique aid automatically")
    arg("-n", "--num_subruns", type=int,default=1, help="number of sub-runs. Each subrun is the same except for the seed")
    arg("-d", "--dry_run", action='store_true', help='Skip slurm submission')

    args = par.parse_args()

    tlim = "48:00:00" if args.tlim is None else args.tlim[0]

    # If the analysis_id is set to -1 then a new directory is built.
    # Else you will conduct a clean-up.
    if args.analysis_id == -1:
        # Get the analysis index to use for the list of jobs.
        analysis_id = get_analysis_id(args.run_names)
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

    for run_name in args.run_names:
        for subrun_id in range(args.num_subruns):
            default_katydid_sub = (
                    f"/opt/python3.7/bin/python3.7 -u /data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_ssa_katydid.py "
                f"-r {run_name} -nid {args.noise_run_id} -aid {analysis_id} -sr {subrun_id} -b \"{args.base_config}\" "
            )
            cmd = apptainer_prefix + f"{default_katydid_sub}'\""
            sbatch_job(run_name, analysis_id, subrun_id, cmd, tlim, args.dry_run)


def sbatch_job(run_name, analysis_id, subrun_id, cmd, tlim, dry_run):
    """
    Replaces SGE qsub with Slurm sbatch.
    Uses --wrap for inline command submission.
    """
    machine_base_path = "/data/raid2/eliza4/he6_cres"
    #machine_base_path = "/Users/buzinsky/Builds/fake_wulf"

    log_path = machine_base_path + f"/spec_sims_analysis/job_logs/katydid/{run_name}_s{subrun_id}_{analysis_id}.txt"

    sbatch_opts = [
        "--job-name", f"{run_name}_s{subrun_id}_{analysis_id}",
        "--time", tlim,
        "--output", log_path,
        "--export=ALL",
        "--mail-type=NONE",
    ]

    sbatch_str = " ".join(sbatch_opts)
    batch_cmd = f"sbatch {sbatch_str} --wrap={cmd}"

    print("\n\n", batch_cmd, "\n\n")
    if not dry_run:
        sp.run(batch_cmd, shell=True)


def get_analysis_id(run_names):
    """
    We want each analysis run simultaneously to have the same analysis number.
    This function goes through and builds the directory structure out and
    checks to see what the next possible analysis index is such that all run_ids
    recieve the same analysis index.

    """
    machine_base_path = Path("/data/raid2/eliza4/he6_cres")
    #machine_base_path = Path("/Users/buzinsky/Builds/fake_wulf")
    base_path = machine_base_path / Path("spec_sims_analysis/root_files")

    max_analysis_ids = []

    for run_name in run_names:
        run_name_dir = base_path / f"r_{run_name}"
        if not run_name_dir.is_dir():
            run_name_dir.mkdir()
            print(f"Created directory: {run_name_dir}")

        # Robust against deleted or missing aids.
        # takes directory names of the form "aid_XXXX", converts it to the list of numbers number
        analysis_ids = [int(re.search(r"aid_(\d+)", str(f)).group(1)) for f in run_name_dir.iterdir() if f.is_dir() ]
        print(f"run_name = {run_name}. Existing aids = {sorted(analysis_ids)}")
        # Use the fact that an empty list is boolean False.
        max_analysis_ids.append(max(analysis_ids) if analysis_ids else 0)

    return max(max_analysis_ids) + 1

if __name__ == "__main__":
    main()
