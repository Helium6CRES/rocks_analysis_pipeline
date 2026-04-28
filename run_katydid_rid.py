#!/usr/bin/env python3
"""
Manages looping over file_ids in a run_id from within apptainer to submit an sbatch for each file
"""

import argparse
import sys
from pathlib import Path
# import glob

from rocks_utility import sbatch_job
from run_katydid_preprocessing import KatydidPreprocessing


def main() -> None:
    par = argparse.ArgumentParser()
    arg =  par.add_argument

    arg("-t", "--tlim", default="48:00:00", type=str, 
        help="set time limit (HH:MM:SS)")
    arg("-rid", "--run_id", type=int, 
        help="run id to analyze")
    arg("-nid", "--noise_run_id", type=int, 
        help="run_id to use for noise floor in katydid run.")
    arg("-b", "--base_config", type=str, 
        help="base .yaml katydid config file to be run on run_id.")
    arg("-fn", "--file_num", default=-1, type=int, 
        help="Number of files in run id to analyze.")
    arg("-ha", "--hold_array", action="store_true",
        help="Hold the submitted array so it does not start immediately.")
    arg("-aid", "--analysis_id", type=int, default=-1, 
        help="analysis_id used to label directories. If -1, a new index will be created.")
    arg("--aid_passed", action="store_true",
        help="Flag to indicate that the user specified aid explicitly, instead the default value. If so, will perform a cleanup if the aid exists or run as normal.")
    arg("-ff", "--fake_field", type=float, 
        help="fake field to use for this run.")
    args = par.parse_args()

    launch_katydid(
        args.tlim,
        args.run_id,
        args.analysis_id,
        args.noise_run_id,
        args.base_config,
        args.file_num,
        args.hold_array,
        args.aid_passed,
        args.fake_field,
    )


def launch_katydid(
    tlim: str, 
    run_id: int,
    analysis_id: int,
    noise_run_id: int,
    base_config: str,
    file_num: int,
    hold_array: bool = False,
    aid_passed: bool = False,
    fake_field: float = None,
) -> None:
    """
    Preprocess run ID then loop over all files in file_df and run Katydid on each as a separate slurm job
    """
    preprocessor = KatydidPreprocessing(
        run_id,
        analysis_id,
        noise_run_id,
        base_config,
        file_num,
        aid_passed,
        fake_field,
    )
    file_df = preprocessor.file_df
    file_df_json_path = preprocessor.file_df_json_path

    condition = (~file_df["root_file_exists"]) & (file_df["exists"])
    print(f"\nRunning katydid on {condition.sum()} of {len(file_df)} files.")

    # Alert which run_ids files do not exist on ROCKS
    no_file_df = file_df.loc[~file_df["exists"], "rocks_file_path"]
    if no_file_df.empty:
        print("All files found on Wulf!")
    else:
        print("The following files don't seem to exist yet on ROCKS!")
        # Print file_id where exists is False
        for rocks_file_path in no_file_df:
            print(rocks_file_path)

    sbatch_katydid_file_array(
        file_df[condition],
        file_df_json_path,
        tlim,
        hold_array,
        fake_field=fake_field,
    )

    # clean_up_root_dir(file_df)


from pathlib import Path


def sbatch_katydid_file_array(
    file_df,
    file_df_json_path: str,
    tlim: str,
    hold_array: bool = False,
    fake_field: float = None,
) -> None:
    n_files = len(file_df)

    run_id = int(file_df["run_id"].iloc[0])
    analysis_id = int(file_df["analysis_id"].iloc[0])

    if fake_field is None and "fake_field" in file_df.columns:
        fake_fields = file_df["fake_field"].dropna().unique()
        if len(fake_fields) == 1:
            fake_field = float(fake_fields[0])

    fake_field_suffix = ""
    if fake_field is not None:
        ff_str = str(fake_field).replace(".", "p")
        fake_field_suffix = f"_ff{ff_str}"

    job_name = f"r{run_id}_a{analysis_id}{fake_field_suffix}"
    log_name = (
        f"rid_{run_id}_aid_{analysis_id}{fake_field_suffix}_fid_%a.txt"
    )
    log_path = (
        f"/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/katydid/{log_name}"
    )

    cmd = (
        f"/opt/python3.7/bin/python3.7 -u "
        f"/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_katydid_file.py "
        f"--file_df_json_path {file_df_json_path} --idx ${{SLURM_ARRAY_TASK_ID}}"
    )

    proc = sbatch_job(
        cmd,
        job_name,
        tlim,
        log_path,
        array=n_files,
        cpus_per_task=1,
        mem=4,
        run_in_apptainer=True,
        hold=hold_array,
    )

    array_jobid = proc.stdout.strip()
    print(f"Submitted array job ID: {array_jobid}")

    if hold_array:
        held_ids_path = Path(
            "/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/katydid/held_array_jobids.txt"
        )
        with held_ids_path.open("a") as f:
            f.write(f"{array_jobid}\n")


def clean_up_root_dir(file_df) -> None:

    # Delete all root files that aren't in our df.
    # TODO: Fix this.

    run_id_aid_dir = Path(file_df["root_file_path"][0]).parents[0]

    real_path_list = run_id_aid_dir.glob("*.root")
    desired_path_list = file_df["root_file_path"].to_list()
    desired_path_list = [Path(path) for path in desired_path_list]
    remove_list = list(set(real_path_list) - set(desired_path_list))

    if len(remove_list) == 0:
        print("Cleaning up root file dir. No files to remove.")
    else:
        print("\nCleaning up. Removing the following files: \n")
        for path in remove_list:
            print(str(path))
            path.unlink()

    # Force a write to the log.
    sys.stdout.flush()


if __name__ == "__main__":
    main()
