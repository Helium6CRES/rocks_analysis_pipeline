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
pd.set_option('display.max_rows', 500)


# Path to imports
sys.path.append("/data/raid2/eliza4/he6_cres/simulation/spec_sims_2026/src")
#sys.path.append("/Users/buzinsky/Builds/spec_sims_SNR/he6-cres-spec-sims/src")
import he6_cres_spec_sims.experiment as exp

############################################################################

def main():
    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument
    arg("-r", "--run_name", type=str, help="labelled run name for MC")
    arg(
        "-nid",
        "--noise_run_id",
        type=int,
        help="run_id to use for noise floor in katydid run. If -1 then will use self as noise file.",
    )
    arg(
        "-y",
        "--yaml_config",
        type=str,
        help="base .yaml config file, should exist in base config directory.",
    )
    arg(
        "-j",
        "--json_config",
        type=str,
        help="base .json config file, should exist in base config directory.",
    )
    arg(
        "-s",
        "--seed",
        type=int,
        help="random seed sent to the Monte Carlo",
    )
    arg(
        "-sr",
        "--subrun_id",
        type=int,
        help="subrun ID [one unique seed per subrun, everything else identical]",
    )

    args = par.parse_args()

    print(f"\nRunning spec-sims. STARTING at PST time: {get_pst_time()}\n")

    # Print summary of spec_sims running.
    print(f"\nProcessing: subrun_id: {args.subrun_id}.\n")

    # Force a write to the log.
    sys.stdout.flush()

    # Done at the beginning and end of main to ensure all users have appropriate access
    set_permissions()

    # Begin running spec-sims
    run_spec_sims = RunSpecSims(
        args.run_name,
        args.subrun_id,
        args.noise_run_id,
        args.yaml_config,
        args.json_config,
        args.seed,
    )

    # set_permissions()

    print(f"\nRunning spec-sims on {args.run_name} {args.subrun_id} DONE at PST time: {get_pst_time()}\n")
    run_spec_sims.run()

    log_file_break()
    return None


class RunSpecSims:
    def __init__(self, run_name, subrun_id, noise_run_id, yaml_config, json_config, seed):

        self.run_name = run_name
        self.subrun_id = subrun_id
        self.noise_run_id = noise_run_id
        self.yaml_config = yaml_config
        self.json_config = json_config
        self.seed = seed

        self.print_run_summary()

        return None

    def print_run_summary(self):
        print("\nRun Summary:")
        print(f"run_name: {self.run_name}")
        print(f"subrun_id: {self.subrun_id}")
        print(f"seed: {self.seed}")
        print(f"noise_run_id: {self.noise_run_id}")
        print(f"yaml_config: {self.yaml_config}")
        print(f"json_config: {self.json_config}\n")
        return None

    def get_base_path(self):
        ### XXX Change base_path location???
        #returns path to directory with configs, confirms configs exist
        base_path = Path("/data/raid2/eliza4/he6_cres/simulation/spec_sims_2026/config_files")
        #base_path = Path("/Users/buzinsky/Builds/spec_sims_SNR/he6-cres-spec-sims/config_files")

        yaml_config_full = base_path / Path(self.yaml_config)
        json_config_full = base_path / Path(self.json_config)

        if not yaml_config_full.is_file():
            raise UserWarning("yaml config doesn't exist")

        if not json_config_full.is_file():
            raise UserWarning("json config doesn't exist")

        return base_path

    # Define a function to aggregate file_path into a list ordered by channel
    def aggregate_paths(self, group):
        ordered_paths = group.sort_values(by='channel')['file_path'].apply(str).tolist()
        return pd.Series({
            'run_id': group['run_id'].iloc[0],
            'true_field': group['true_field'].iloc[0],
            'file_path': ordered_paths
        })

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

    def run(self):
        # Force a write to the log.
        sys.stdout.flush()

        base_path = self.get_base_path()
        yaml_config_full = base_path / Path(self.yaml_config)
        json_config_full = base_path / Path(self.json_config)

        #Load the yaml configuration file
        with open(yaml_config_full, "r") as f:
            try:
                yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
            except yaml.YAMLError as e:
                print(e)

        # Open the JSON file and load its content into a dictionary
        with open(json_config_full, "r") as f_json_config:
            run_params = json.load(f_json_config)

        ##### run_params format
        #run_params = {
        #    "experiment_name": "RUN_NAME",
        #    "base_config_path": "YAML_NAME",
        #    "events_to_simulate": -1,
        #    "betas_to_simulate": 1000,
        #    "isotope": "Ne19",
        #    "rand_seeds": rand_seeds,
        #    "fields_T" : fields.tolist(),
        #    "traps_A": traps.tolist()
        #}

        run_params["experiment_name"] = self.run_name
        run_params["base_config_path"] = str(yaml_config_full)
        run_params["rand_seeds"] = [self.seed] * len(run_params["fields_T"])


        #define where the MC results are going to be written to
        base_run_dir = Path("/data/raid2/eliza4/he6_cres/simulation/sim_results/runs")
        #base_run_dir = Path("/Users/buzinsky/Builds/spec_sims_SNR/he6-cres-spec-sims/config_files/tmp")

        base_experiment_dir = base_run_dir / Path(self.run_name)
        print(base_experiment_dir)

        ## Make the base_run_dir if it doesn't exist
        if not base_experiment_dir.is_dir():
            base_experiment_dir.mkdir()
            print("Created directory: {} ".format(base_experiment_dir))

        run_params["output_path"] = base_experiment_dir / Path(f"subrun_{self.subrun_id}")
        print(run_params["output_path"])

        if yaml_dict["Settings"]["sim_daq"] == True:
            yaml_dict["DAQ"]["noise_paths"] = self.get_noise_fp()

        print(yaml_dict)

        t_start = time.process_time()

        ####################Do the Run!##############################
        for key, val in run_params.items():
            print("{}: {}".format(key, val))

        #exp.Experiment(run_params)
        exp.Experiment(run_params,yaml_dict)
        #############################################################
        t_stop = time.process_time()
        elapsed = t_stop - t_start
        print(elapsed)

        return None

if __name__ == "__main__":
    main()
