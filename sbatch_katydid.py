#!/usr/bin/env python3
"""
CLI entry point for submitting katydid runs via sbatch. Misleadingly named, this actually starts run_katydid() in the apptainer which then handles looping over run_ids and submitting an sbatch for each file
"""
import subprocess as sp
import argparse
from pathlib import Path

def main():
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-t", "--tlim", nargs=1, default="48:00:00", type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--runids", nargs="+", type=int, help="run ids to analyze")
    arg("-nid", "--noise_run_id", type=int, help="run_id to use for noise floor in katydid run.")
    arg("-b", "--base_config", type=str, help="base .yaml katydid config file to be run on run_id.")
    arg("-fn", "--file_num", default=-1, type=int, help="Number of files in run id to analyze.")
    arg("-aid", "--analysis_id", type=int, default=-1, help="analysis_id used to label directories. If -1, a new index will be created.")

    args = par.parse_args()

    if not args.runids:
        raise ValueError("Must provide --runids")

    # If the analysis_id is set to -1 then a new directory is built.
    # Else you will conduct a clean-up.
    if args.analysis_id == -1:
        # Get the analysis index to use for the list of jobs.
        analysis_id = get_analysis_id(args.runids)
        print(f"analysis_id: {analysis_id}")
    else:
        analysis_id = args.analysis_id

    launch_katydid(
            args.tlim,
            args.runids,
            analysis_id,
            args.noise_run_id,
            args.base_config,
            args.file_num,
            )


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
            run_id_dir.mkdir(parents = True, exist_ok = True)
            print(f"Created directory: {run_id_dir}")
        
        # Robust against deleted or missing aids.
        analysis_ids = [
            int(f.name[-3:]) for f in run_id_dir.iterdir() if f.is_dir()
        ]
        print(f"run_id = {run_id}. Existing aids = {sorted(analysis_ids)}")
        # Use the fact that an empty list is boolean False.
        max_analysis_ids.append(max(analysis_ids) if analysis_ids else 0)

    return max(max_analysis_ids) + 1

def launch_katydid(
        tlim,
        run_ids,
        analysis_id,
        noise_run_id,
        base_config,
        file_num,
        ):

    # Build the command to run inside the container
    container_cmd = (
        f"umask 002; "
        f"source /data/raid2/eliza4/he6_cres/.bashrc; "
        f"/opt/python3.7/bin/python3.7 -u "
        f"/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/launch_katydid.py "
        f"-t {tlim} -rids {run_ids} -nid {noise_run_id} -aid {analysis_id} -b {base_config} -fn {file_num}"
    )

    # Full Apptainer command
    apptainer_cmd = (
        "apptainer exec "
        "--bind /data/raid2/eliza4/he6_cres/:/data/raid2/eliza4/he6_cres/ "
        "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
        f"/bin/bash -c \"{container_cmd}\""
    )

    cmd = f"{apptainer_cmd}"
    sp.run(cmd, shell = True)

if __name__ == "__main__":
    main()

