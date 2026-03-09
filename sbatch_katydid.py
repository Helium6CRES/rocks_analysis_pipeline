#!/usr/bin/env .venv/bin/python3
"""
CLI entry point for submitting katydid runs via sbatch. 
"""
import argparse
from pathlib import Path
from rocks_utility import sbatch_job

def main():
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-t", "--tlim", default="48:00:00", type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--runids", nargs="+", type=int, help="run ids to analyze")
    arg("-nid", "--noise_run_id", type=int, help="run_id to use for noise floor in katydid run.")
    arg("-b", "--base_config", type=str, help="base .yaml katydid config file to be run on run_id.")
    arg("-fn", "--file_num", default=-1, type=int, help="Number of files in run id to analyze.")
    arg("-aid", "--analysis_id", type=int, default=-1, help="analysis_id used to label directories. If -1, a new index will be created.")

    args = par.parse_args()

    if not args.runids:
        raise ValueError("Must provide --runids")

    # Get the analysis index to use for the list of jobs.
    # If the analysis_id is set to -1 (default) then the most recent analysis is overwritten, or a new directory is built if none exist.
    # If the analysis_id does not exist yet, then the analysis runs as normal with that analysis_id.
    # Else you will conduct a clean-up.
    if args.analysis_id == -1:
        analysis_id = get_max_analysis_id(args.runids)
        print(f"analysis_id: {analysis_id}")
        aid_passed = False
    else:
        analysis_id = args.analysis_id
        aid_passed = True

    for run_id in args.runids:
        sbatch_katydid(
                args.tlim,
                run_id,
                analysis_id,
                args.noise_run_id,
                args.base_config,
                args.file_num,
                aid_passed,
                )


def get_max_analysis_id(run_ids):
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
            run_id_dir.mkdir(parents = True, exist_ok = True)
            print(f"Created directory: {run_id_dir}")
        
        # Robust against deleted or missing aids.
        analysis_ids = [
            int(f.name[-3:]) for f in run_id_dir.iterdir() if f.is_dir()
        ]
        print(f"run_id = {run_id}. Existing aids = {sorted(analysis_ids)}")
        # Use the fact that an empty list is boolean False.
        max_analysis_ids.append(max(analysis_ids) if analysis_ids else 0)

    return max(max_analysis_ids)

def sbatch_katydid(
        tlim,
        run_id,
        analysis_id,
        noise_run_id,
        base_config,
        file_num,
        aid_passed=False,
        ):

    base_dir = Path("/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline")
    python_venv = base_dir / ".venv/bin/python"
    script = base_dir / "run_katydid_rid.py"
    args = (
        f"-t {tlim} "
        f"-rid {run_id} "
        f"-nid {noise_run_id} "
        f"-aid {analysis_id} "
        f"-b {base_config} "
        f"-fn {file_num} "
        )

    if aid_passed:
        args += "--aid_passed "

    cmd = f"{python_venv} -u {script} {args}"

    job_name = f"r{run_id}_a{analysis_id}"
    log_name = f"rid_{run_id}_aid_{analysis_id}.txt"
    log_path = f"/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/katydid/{log_name}"

    sbatch_job(cmd, job_name, tlim, log_path)

if __name__ == "__main__":
    main()

