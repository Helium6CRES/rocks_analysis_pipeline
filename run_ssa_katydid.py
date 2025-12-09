#!/usr/bin/env python3
import os
import time
import argparse
import pandas as pd
import datetime
from glob import glob

from shutil import copyfile
import psycopg2
from psycopg2 import Error
import typing
from typing import List
import pandas.io.sql as psql
from pathlib import Path
import yaml
import sys
import subprocess as sp
import json
import re

# Local imports.
sys.path.append("/data/raid2/eliza4/he6_cres/simulation/he6-cres-spec-sims/src")
#sys.path.append("/Users/buzinsky/Builds/spec_sims_SNR/he6-cres-spec-sims/src")

import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc

# Local imports.
from rocks_utility import (
    he6cres_db_query,
    get_pst_time,
    set_permissions,
    check_if_exists,
    log_file_break,
)

# Import settings.
pd.set_option("display.max_columns", 100)

def main():
    """
    DOCUMENT

    """

    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument
    arg("-r", "--run_name", type=str, help="spec_sims run_name to run katydid on.")
    arg( "-nid", "--noise_run_id", type=int, help="run_id to use for noise floor in katydid run. If -1 then will use self as noise file.",)
    arg( "-aid", "--analysis_id", type=int, help="analysis_id used to label directories.",)
    arg( "-b", "--base_config", type=str, help="base .yaml katydid config file to be run on run_id, should exist in base config directory.",)
    args = par.parse_args()

    print(f"\nRunning Katydid. STARTING at PST time: {get_pst_time()}\n")

    # Print summary of katydid running.
    print(f"\nProcessing: run_name: {args.run_name}.\n")

    # Force a write to the log.
    sys.stdout.flush()

    # Done at the beginning and end of main to ensure all users have
    # appropriate access.
    set_permissions()

    # Begin running Katydid.
    run_katydid = RunKatydid(
        args.run_name,
        args.analysis_id,
        args.noise_run_id,
        args.base_config,
    )

    # set_permissions()

    print(f"\nRunning Katydid on {args.run_name} DONE at PST time: {get_pst_time()}\n")
    log_file_break()
    return None


class RunKatydid:
    def __init__(self, run_name, analysis_id, noise_run_id, base_config):

        self.run_name = run_name
        self.analysis_id = analysis_id
        self.noise_run_id = noise_run_id
        self.base_config = base_config

        self.machine_path = Path("/data/raid2/eliza4/he6_cres")
        #self.machine_path = Path("/Users/buzinsky/Builds/fake_wulf")

        # Step 0. Print run summary.
        self.print_run_summary()

        # Step 1. Build the path to the file_df.
        self.file_df_path = self.build_file_df_path()

        # Step 2. Collect the file_df. This means deciding if cleanup or new analysis.
        self.file_df = self.collect_file_df()

        # Step 3. Run katydid on all files in file_df that don't already have root files
        condition = (self.file_df["root_file_exists"] != True)
        print(f"\nRunning katydid on {condition.sum()} of {len(self.file_df)} files.")
        # Run katydid on each row/spec file in file_df.
        for idx, row in self.file_df[condition].iterrows():
            #try:
            print(f"\nProcessing file_id {row['file_id']} at PST time: {get_pst_time()}")
            sys.stdout.flush()
            self.run_katydid(row)
            print(f"Finished file_id {row['file_id']} at PST time: {get_pst_time()}")
            sys.stdout.flush()
            #except Exception as e:
            #    print(f"Exception while processing file_id {row['file_id']}: {e}")
            #    sys.stdout.flush()
            #    continue

        # Clean up any half baked root files.
        self.clean_up_root_dir(self.file_df)

        # set_permissions()

        return None

    def print_run_summary(self):
        print("\nRun Summary:")
        print(f"run_name: {self.run_name}")
        print(f"analysis_id: {self.analysis_id}")
        print(f"base_config: {self.base_config}\n")
        return None

    def build_file_df_path(self):
        base_path = self.machine_path / Path("spec_sims_analysis/root_files")

        rid_ai_dir = base_path / Path(f"r_{self.run_name}") / Path(f"aid_{self.analysis_id}")
        file_df_path = rid_ai_dir / Path( f"rid_df_{self.run_name}_{self.analysis_id}.csv")
        return file_df_path


    def collect_file_df(self):
        # This function figures out if the run is a clean up or new analysis
        # and collects the file_df.
        # This entails determining if this should be a clean-up or new analysis.
        # If the rid_df already exists it's a cleanup

        # Clean up.
        if self.file_df_path.is_file():
            print( f"Analysis Type: Clean up. \nfile_df {self.file_df_path} already exists.\n")

            file_df = pd.read_csv(self.file_df_path)
            file_df["rocks_file_path"] = file_df["rocks_file_path"].apply(json.loads)
            file_df["rocks_noise_file_path"] = file_df["rocks_noise_file_path"].apply(json.loads)

            # Check to see which root files already exist.
            file_df["root_file_exists"] = file_df["root_file_path"].apply( lambda x: check_if_exists(x))

        # New analysis.
        else:
            print(f"Analysis Type: New analysis. \nBuilding file_df.\n")
            file_df = self.build_full_file_df()
        return file_df

    def build_full_file_df(self):
        file_df = self.create_base_file_df()

        file_df["analysis_id"] = self.analysis_id
        file_df["root_file_exists"] = False
        file_df["file_id"] = file_df.index

        file_df["approx_slope"] = self.get_slope(file_df["true_field"][0])

        file_df["base_config_path"] = self.get_base_config_path()
        #spec_sims_analysis/root_files/r_run_name
        file_df["output_dir"] = self.build_dir_structure()

        # Collect either the given noise id or assign 'self' to noise file path.
        if self.noise_run_id == -1:
            print("\nUsing 'self' as noise file in katydid analysis.\n")
            file_df["rocks_noise_file_path"] = file_df["rocks_file_path"].apply(json.loads)
        else:
            noise_fp_list = self.get_noise_fp()
            print("Noise file list: ", noise_fp_list)
            file_df["rocks_noise_file_path"] = [noise_fp_list] * len(file_df)

        print(file_df)

        file_df["root_file_path"] = file_df.apply( lambda row: self.build_slew_root_filename(row,".root"), axis=1)
        file_df["slew_file_path"] = file_df.apply( lambda row: self.build_slew_root_filename(row,"_SlewTimes.txt"), axis=1)

        # Before running katydid write this df to the analysis dir.
        # This will be used during the cleanup run.
        print(f"Built file_df: {self.file_df_path}")

        # CSV-only copy so we don't mutate the in-memory df we return
        file_df_csv = file_df.copy()
        file_df_csv["rocks_file_path"] = file_df_csv["rocks_file_path"].map(json.dumps)
        file_df_csv["rocks_noise_file_path"] = file_df_csv["rocks_noise_file_path"].map(json.dumps)
        file_df_csv.to_csv(self.file_df_path, index=False)

        return file_df

    def create_base_file_df(self):
        #trawls through spec-sims/sim_results/run_name, get all the speck files. Initialize w/ columns:
        #rocks_file_path, run_name, subrun_id, spec_sims_yaml, seed, main_field, trap_current
        print(str(self.machine_path) + "/simulation/sim_results/" + self.run_name + "/subrun_*/*/*.speck")
        speck_files = glob(str(self.machine_path) + "/simulation/sim_results/" + self.run_name + "/subrun_*/*/spec_files/*.speck")

        print("Found speck files: ", speck_files)

        #regular expression parse file paths for info about runs. (Pulls integers \d+ from parentheses matching pattern)
        subrun_ids = [int(re.search(r"subrun_(\d+)", p).group(1)) for p in speck_files]
        acqs = [int(re.search(r"(\d+)_\d+\.speck$", Path(p).name).group(1)) for p in speck_files]
        channels = [int(re.search(r"_(\d+)\.speck$", Path(p).name).group(1)) for p in speck_files]

        print("subrun_ids", subrun_ids )
        print("acqs:", acqs)
        print("channels", channels )

        #*paths are path objects, *_files are same, but string objects
        #p.parents[1] is string like "0_field_1.92223T". We want that yaml file, which ran the MC
        speck_file_paths = [Path(s) for s in speck_files]
        yaml_file_paths = [p.parents[1].parent / (p.parents[1].name + ".yaml") for p in speck_file_paths]
        yaml_files = [str(yfp) for yfp in yaml_file_paths]

        file_df = pd.DataFrame(speck_files, columns=["rocks_file_path"])
        file_df["subrun_id"] = subrun_ids
        file_df["acquisition"] = acqs
        file_df["channel"] = channels
        file_df["spec_sims_yaml"] = yaml_files

        #merge rocks_file_path's into a list for rows with the same field, subrun, acq.
        file_df = (
            file_df.sort_values("channel")
              .groupby(["spec_sims_yaml", "subrun_id","acquisition"])
              .agg(rocks_file_path=("rocks_file_path", list))
              .reset_index()
        )

        file_df["run_name"] = self.run_name
        file_df["analysis_id"] = self.analysis_id

        #for each yaml, load, get seeds, main_fields, trap currents
        seeds = []
        main_fields = []
        trap_currents = []

        unique_yamls = set(yaml_files)

        for yaml_config in unique_yamls:
            with open(yaml_config, "r") as f:
                try:
                    spec_sim_config_dict = yaml.load(f, Loader=yaml.FullLoader)
                    cond = (file_df["spec_sims_yaml"] == yaml_config)
                    file_df.loc[cond, 'seed'] = spec_sim_config_dict["Settings"]["rand_seed"]
                    file_df.loc[cond, 'true_field'] = spec_sim_config_dict["EventBuilder"]["main_field"]
                    file_df.loc[cond, 'trap_current'] = spec_sim_config_dict["EventBuilder"]["trap_current"]
                except yaml.YAMLError as e:
                    print(e)
                    print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        print(file_df)

        # Group by file_inAcq and apply the aggregation function XXX (what do?)
        #file_df = file_df.groupby('file_in_acq').apply(self.aggregate_paths).reset_index(drop=True)
        return file_df

    # Define a function to aggregate file_path into a list ordered by channel
    def aggregate_paths(self, group):
        ordered_paths = group.sort_values(by='channel')['file_path'].apply(str).tolist()
        return pd.Series({
            'run_id': group['run_id'].iloc[0],
            'true_field': group['true_field'].iloc[0],
            'file_path': ordered_paths
        })

    def get_slope(self, true_field, frequency: float = 19.15e9):

        approx_power = sc.power_larmor(true_field, frequency)
        approx_energy = sc.freq_to_energy(frequency, true_field)
        approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

        return approx_slope

    def get_base_config_path(self):
        base_config_full = self.machine_path / Path(f"spec_sims_analysis/base_configs/{self.base_config}")

        if not base_config_full.is_file():
            raise UserWarning("base config doesn't exist. ")

        return str(base_config_full)

    def build_dir_structure(self):

        base_path = self.machine_path / Path("spec_sims_analysis/root_files")
        run_name_dir = base_path / Path(f"r_{self.run_name}")

        if not run_name_dir.is_dir():
            raise UserWarning("This directory should have been made already.")

        current_analysis_dir = run_name_dir / Path(f"aid_{self.analysis_id}")
        if not current_analysis_dir.is_dir():
            current_analysis_dir.mkdir()
            print(f"Created directory: {current_analysis_dir}")

        return str(current_analysis_dir)

    def get_noise_fp(self):
        """
        Note: just takes the first file in this run_id (assumption is it's a one file acq)
        """
        query_he6_db = """
                        SELECT f.run_id, f.file_path, f.file_in_acq, f.channel
                        FROM he6cres_runs.spec_files as f
                        WHERE f.run_id = {}
                        ORDER BY f.channel
                        LIMIT 2
                      """.format(
            self.noise_run_id
        )

        noise_file_df = he6cres_db_query(query_he6_db)

        # Group by file_inAcq and apply the aggregation function
        #make dummy true_field column to use agg function. this is dumb fix later
        noise_file_df["true_field"] = 0
        noise_file_df = noise_file_df.groupby('file_in_acq').apply(self.aggregate_paths).reset_index(drop=True)

        noise_file_path = noise_file_df["file_path"].iloc[0]
        print(f"Noise path: {noise_file_path}")

        #Convert to directory structure on wulf
        wulf_noise_paths = [Path("/data/raid2/eliza4/he6_cres/") / Path(old).relative_to("/mnt") for old in noise_file_path]
        #wulf_noise_paths = [Path("/Users/buzinsky/Builds/DAQ/He6DAQ/pyqt5_GUI/temp/") / Path(old).name for old in noise_file_path]
        print(wulf_noise_paths)

        for noise_file in wulf_noise_paths:
            if not noise_file.is_file():
                raise UserWarning(f"{noise_file} doesn't exist")

        str_wulf_noise_paths = [str(wnp) for wnp in wulf_noise_paths]

        return str_wulf_noise_paths

    #footer (_SlewTimes.txt, .root)
    def build_slew_root_filename(self, file_df, footer):
        root_path = file_df["output_dir"] + "/"
        root_path += str(file_df["subrun_id"]) + "_"
        root_path += str(file_df["true_field"]) + "T_"
        root_path += str(file_df["acquisition"])
        #+ str(file_df["analysis_id"]) + "_" #we already know the analysis id from the directory! redundant!
        root_path += footer
        return root_path

    def run_katydid(self, file_df):

        # Force a write to the log.
        sys.stdout.flush()

        base_config_path = file_df["base_config_path"]

        run_name = file_df["run_name"]
        analysis_id = file_df["analysis_id"]

        #config_path = base_config_path.parent / str( base_config_path.stem + f"_{run_name}_{analysis_id}" + base_config_path.suffix)
        katydid_command_list = ["/data/raid2/eliza4/he6_cres/katydid/build/bin/Katydid", "-c", base_config_path]

        for i in range(2):
            katydid_command_list.append(f"--spec1.filenames.{i}="+file_df["rocks_noise_file_path"][i])

        for i in range(2):
            katydid_command_list.append(f"--spec2.filenames.{i}="+file_df["rocks_file_path"][i])

        katydid_command_list.append("--long-tr-find.initial-slope="+str(file_df["approx_slope"]))
        katydid_command_list.append("--long-tr-find.min-slope="+str(file_df["approx_slope"] - 1e10))

        katydid_command_list.append("--rtw.output-file="+file_df["root_file_path"])
        katydid_command_list.append("--brw.output-file="+file_df["root_file_path"])
        katydid_command_list.append("--stv.output-file="+file_df["slew_file_path"])

        # Run katydid on the edited katydid config file.
        # Note that you need to have Katydid configured as a bash executable for this to
        # work (as is standard).
        print(katydid_command_list)
        t_start = time.process_time()

        proc = sp.run(
            katydid_command_list,
            capture_output=True,
        )

        # Decode logs (avoid escape noise)
        out = proc.stdout.decode(errors="replace")
        err = proc.stderr.decode(errors="replace")

        print("Katydid stdout (tail 1k):", out[-1000:])
        if err.strip():
            print("Katydid stderr (tail 1k):", err[-1000:])

        t_stop = time.process_time()
        elapsed = t_stop - t_start

        root_path = Path(file_df["root_file_path"])
        root_exists = root_path.is_file()
        root_size = root_path.stat().st_size if root_exists else 0

        # Only claim success if returncode==0 and the file exists and is non-empty
        if proc.returncode == 0 and root_exists and root_size > 0:
            print(
                f"\nfile {file_df['file_id']}."
                f"\ntime to run: {elapsed:.2f} s."
                f"\ncurrent time: {get_pst_time()}."
                f"\nroot file created {root_path}\n"
            )
        else:
            print(
                f"\nfile {file_df['file_id']} FAILED."
                f"\ntime: {elapsed:.2f} s."
                f"\nreturncode: {proc.returncode}"
                f"\nroot exists: {root_exists} size: {root_size}\n"
                f"Config kept for debug: {config_path}\n"
            )

        return None

    def clean_up_root_dir(self, file_df):

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
                #path.unlink()

        # Force a write to the log.
        sys.stdout.flush()

        return None


if __name__ == "__main__":
    main()
