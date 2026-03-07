#!/usr/bin/env python3
"""
Manages looping over file_ids from within apptainer to submit an sbatch for each file
"""

import argparse
import sys
from pathlib import Path
# import glob

from rocks_utility import sbatch_job
from run_katydid_preprocessing import KatydidPreprocessing

def main():
    par = argparse.ArgumentParser()
    arg =  par.add_argument

    arg("-t", "--tlim", default="48:00:00", type=str, help="set time limit (HH:MM:SS)")
    arg("-rid", "--run_id", type=int, help="run id to analyze")
    arg("-nid", "--noise_run_id", type=int, help="run_id to use for noise floor in katydid run.")
    arg("-b", "--base_config", type=str, help="base .yaml katydid config file to be run on run_id.")
    arg("-fn", "--file_num", default=-1, type=int, help="Number of files in run id to analyze.")
    arg("-aid", "--analysis_id", type=int, default=-1, help="analysis_id used to label directories. If -1, a new index will be created.")

    args = par.parse_args()

    launch_katydid(
            args.tlim,
            args.run_id,
            args.analysis_id,
            args.noise_run_id,
            args.base_config,
            args.file_num,
            )

def launch_katydid(tlim, run_id, analysis_id, noise_run_id, base_config, file_num):
    """
    Preprocess run ID then loop over all files in file_df and run Katydid on each as a separate slurm job
    """
    preprocessor = KatydidPreprocessing(
        run_id,
        analysis_id,
        noise_run_id,
        base_config,
        file_num,
    )
    file_df = preprocessor.file_df
    file_df_json_path = preprocessor.file_df_json_path

    condition = (~file_df["root_file_exists"]) & (file_df["exists"])
    print(f"\nRunning katydid on {condition.sum()} of {len(file_df)} files.")

    # Alert which run_ids files do not exist on ROCKS
    no_file_df = file_df.loc[~file_df['exists'], 'rocks_file_path']
    if no_file_df.empty:
        print("All files found on Wulf!")
    else:
        print("The following files don't seem to exist yet on ROCKS!")
        # Print file_id where exists is False
        for rocks_file_path in no_file_df:
            print(rocks_file_path)

    for idx, row in file_df[condition].iterrows():
        sbatch_katydid_file(file_df_json_path, row, idx, tlim)

    # clean_up_root_dir(file_df)

def sbatch_katydid_file(file_df_json_path, file_df_row, row_idx, tlim):
    run_id = file_df_row["run_id"]
    analysis_id = file_df_row["analysis_id"]
    file_id = file_df_row["file_id"]

    job_name = f"r{run_id}_a{analysis_id}_f{file_id}"
    log_name = f"rid_{run_id}_aid_{analysis_id}_fid_{file_id}.txt"
    log_path = f"/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/katydid/{log_name}"

    cmd = (
        f"/opt/python3.7/bin/python3.7 -u "
        f"/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_katydid_file.py "
        f"--file_df_json_path {file_df_json_path} --idx {row_idx}"
    )

    sbatch_job(cmd, job_name, tlim, log_path, run_in_apptainer = True)

def clean_up_root_dir(file_df):

    # Delete all root files that aren't in our df.
    # TODO: Fix this.
    # Luciano TODO: rewrite this so it deletes the last analysis id unless the user requests not to

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
