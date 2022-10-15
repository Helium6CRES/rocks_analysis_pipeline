#!/usr/bin/env python3
import os
import time
import argparse
import pandas as pd
import datetime
from glob import glob
import subprocess as sp
from shutil import copyfile
import psycopg2
from psycopg2 import Error
import typing
from typing import List
import pandas.io.sql as psql
from pathlib import Path
import yaml
import sys
import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc

# Local imports.
from rocks_utility import (
    he6cres_db_query,
    get_pst_time,
    set_permissions,
    check_if_exists,
    log_file_break
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
    arg("-id", "--run_id", type=int, help="run_id to run katydid on.")
    arg(
        "-nid",
        "--noise_run_id",
        type=int,
        help="run_id to use for noise floor in katydid run.",
    )
    arg(
        "-aid",
        "--analysis_id",
        type=int,
        help="analysis_id used to label directories.",
    )
    arg(
        "-b",
        "--base_config",
        type=str,
        help="base .yaml katydid config file to be run on run_id, should exist in base config directory.",
    )
    arg(
        "-fn",
        "--file_num",
        default=-1,
        type=int,
        help="Number of files in run id to analyze (<= number of files in run_id)",
    )

    args = par.parse_args()

    print(f"\nRunning Katydid. STARTING at PST time: {get_pst_time()}\n")

    # Print summary of katydid running.
    print(f"\nProcessing: run_id: {args.run_id}.\n")

    # Force a write to the log.
    sys.stdout.flush()

    # Done at the beginning and end of main to ensure all users have
    # appropriate access.
    set_permissions()

    # Begin running Katydid.
    run_katydid = RunKatydid(
        args.run_id,
        args.analysis_id,
        args.noise_run_id,
        args.base_config,
        args.file_num,
    )

    set_permissions()

    print(f"\nRunning Katydid on {args.run_id} DONE at PST time: {get_pst_time()}\n")
    log_file_break()
    return None


class RunKatydid:
    def __init__(self, run_id, analysis_id, noise_run_id, base_config, file_num):

        self.run_id = run_id
        self.analysis_id = analysis_id
        self.noise_run_id = noise_run_id
        self.base_config = base_config
        self.file_num = file_num

        # Step 0. Print run summary.
        self.print_run_summary()

        # Step 1. Build the path to the file_df.
        self.file_df_path = self.build_file_df_path()

        # Step 2. Collect the file_df. This means deciding if cleanup or new analysis.
        self.file_df = self.collect_file_df()

        # Step 3. Run katydid on all files in file_df that don't already exist.
        condition = self.file_df["root_file_exists"] != True
        print(f"\nRunning katydid on {condition.sum()} of {len(self.file_df)} files.")
        # Run katydid on each row/spec file in file_df.
        self.file_df[condition].apply(lambda row: self.run_katydid(row), axis=1)

        # Clean up any half baked root files.
        self.clean_up_root_dir(self.file_df)

        set_permissions()

        return None

    def print_run_summary(self):
        print("\nRun Summary:")
        print(f"run_id: {self.run_id}")
        print(f"analysis_id: {self.analysis_id}")
        print(f"base_config: {self.base_config}\n")
        return None

    def collect_file_df(self):
        # This function figures out if the run is a clean up or new analysis
        # and collects the file_df.
        # This entails determining if this should be a clean-up or new analysis.
        # If the rid_df already exists it's a cleanup

        # Clean up.
        if self.file_df_path.is_file():
            print(f"Analysis Type: Clean up. \nfile_df {self.file_df_path} already exists.\n")

            file_df = pd.read_csv(self.file_df_path)

            # Check to see which root files already exist.
            file_df["root_file_exists"] = file_df["root_file_path"].apply(
                lambda x: check_if_exists(x)
            )

        # New analysis.
        else:
            print(f"Analysis Type: New analysis. \nBuilding file_df.\n")
            file_df = self.build_full_file_df()
        return file_df

    def build_file_df_path(self):
        base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")
        rid_ai_dir = (
            base_path
            / Path(f"rid_{self.run_id:04d}")
            / Path(f"aid_{self.analysis_id:03d}")
        )

        file_df_path = rid_ai_dir / Path(
            f"rid_df_{self.run_id:04d}_{self.analysis_id:03d}.csv"
        )
        return file_df_path

    def build_full_file_df(self):

        file_df = self.create_base_file_df(self.run_id)
        file_df["analysis_id"] = self.analysis_id
        file_df["root_file_exists"] = False
        file_df["file_id"] = file_df.index
        file_df["rocks_file_path"] = file_df["file_path"].apply(
            lambda x: self.process_fp(x)
        )
        file_df["exists"] = file_df["rocks_file_path"].apply(
            lambda x: check_if_exists(x)
        )
        file_df["approx_slope"] = self.get_slope(file_df["true_field"][0])

        dbscan_r = self.get_dbscan_radius(file_df["approx_slope"][0])
        file_df["dbscan_radius_0"] = dbscan_r[0]
        file_df["dbscan_radius_1"] = dbscan_r[1]

        file_df["base_config_path"] = self.get_base_config_path()
        file_df["output_dir"] = self.build_dir_structure()

        file_df["noise_file_path"] = self.get_noise_fp()
        file_df["rocks_noise_file_path"] = file_df["noise_file_path"].apply(
            lambda x: self.process_fp(x)
        )

        file_df["root_file_path"] = file_df.apply(
            lambda row: self.build_root_file_path(row), axis=1
        )

        # Trim the df according to the file_num arg.
        if self.file_num != -1:
            file_df = file_df[:self.file_num]

        # Before running katydid write this df to the analysis dir.
        # This will be used during the cleanup run.
        print(f"Built file_df: {self.file_df_path}")
        file_df.to_csv(self.file_df_path)

        return file_df

    def create_base_file_df(self, run_id: int):
        # DOCUMENT. 
        query_he6_db = """
                        SELECT r.run_id, f.spec_id, f.file_path, r.true_field
                        FROM he6cres_runs.run_log as r
                        RIGHT JOIN he6cres_runs.spec_files as f
                        ON r.run_id = f.run_id
                        WHERE r.run_id = {}
                        ORDER BY r.created_at DESC
                      """.format(
            run_id
        )

        file_df = he6cres_db_query(query_he6_db)

        return file_df

    def process_fp(self, daq_fp):
        rocks_fp = "/data/eliza4/he6_cres/" + daq_fp[5:]
        return rocks_fp

    def get_slope(self, true_field, frequency: float = 18.5e9):

        approx_power = sc.power_larmor(true_field, frequency)
        approx_energy = sc.freq_to_energy(frequency, true_field)
        approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

        return approx_slope

    def get_dbscan_radius(
        self, approx_slope: float, dbscan_base_radius: List[float] = [5.0e-4, 40e6]
    ) -> List[float]:

        dbscan_radius = [
            dbscan_base_radius[1] / approx_slope,
            dbscan_base_radius[0] * approx_slope,
        ]

        return dbscan_radius

    def get_base_config_path(self):

        base_path = Path("/data/eliza4/he6_cres/katydid_analysis/base_configs")
        base_config_full = base_path / Path(self.base_config)

        if not base_config_full.is_file():
            raise UserWarning("base config doesn't exist. ")

        return str(base_config_full)

    def build_dir_structure(self):

        base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")

        run_id_dir = base_path / Path(f"rid_{self.run_id:04d}")

        if not run_id_dir.is_dir():
            raise UserWarning("This directory should have been made already.")

        current_analysis_dir = run_id_dir / Path(f"aid_{self.analysis_id:03d}")
        if not current_analysis_dir.is_dir():
            current_analysis_dir.mkdir()
            print(f"Created directory: {current_analysis_dir}")

        return str(current_analysis_dir)

    def get_noise_fp(self):
        """
        DOCUMENT
        Note: just takes the first file in this run_id (assumption is it's a one file acq)
        """
        query_he6_db = """
                        SELECT f.run_id, f.file_path 
                        FROM he6cres_runs.spec_files as f
                        WHERE f.run_id = {}
                        ORDER BY f.created_at DESC
                        LIMIT 1
                      """.format(
            self.noise_run_id
        )

        file_df = he6cres_db_query(query_he6_db)

        noise_file_path = file_df["file_path"].iloc[0]
        print(f"Noise path: {noise_file_path}")

        return noise_file_path

    def build_root_file_path(self, file_df):
        root_path = Path(file_df["output_dir"]) / str(
            Path(file_df["rocks_file_path"]).stem + file_df["output_dir"][-4:] + ".root"
        )

        return str(root_path)

    def run_katydid(self, file_df):

        # Force a write to the log.
        sys.stdout.flush()

        base_config_path = Path(file_df["base_config_path"])

        # Grab the config_dict from the katydid config file.
        with open(base_config_path, "r") as f:
            config_dict = yaml.load(f, Loader=yaml.FullLoader)

        # Copy the katydid config file (in same dir) so that we can write to the copy not
        # the original.
        rid = file_df["run_id"]
        aid = file_df["analysis_id"]

        config_path = base_config_path.parent / str(
            base_config_path.stem + f"_{rid:04d}_{aid:03d}" + base_config_path.suffix
        )

        # copy base config file to edit
        copyfile(base_config_path, config_path)

        # copy first config file to the analysis directory for future reference.
        if file_df["file_id"] == 0:
            analysis_dir = Path(file_df["root_file_path"]).parents[0]
            config_path_name = Path(config_path).name
            saved_config_path = analysis_dir / config_path_name
            copyfile(config_path, saved_config_path)

            print(
                f"Writing the config file used in analysis to disk here: \n {str(saved_config_path)}\n"
            )

        config_dict["spec1"]["filename"] = file_df["rocks_noise_file_path"]
        config_dict["spec2"]["filename"] = file_df["rocks_file_path"]

        for key, val in config_dict.items():
            for inner_key, inner_val in val.items():
                if inner_key == "output-file":
                    config_dict[key][inner_key] = file_df["root_file_path"]

                if inner_key == "initial-slope":

                    config_dict[key][inner_key] = file_df["approx_slope"]

                if inner_key == "radii":
                    config_dict[key][inner_key] = [
                        file_df["dbscan_radius_0"],
                        file_df["dbscan_radius_1"],
                    ]

        # Dump the altered config_dict into the copy of the config file.
        # Note that the comments are all lost because you only write the contents of the
        # confic dict.
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

        # Run katydid on the edited katydid config file.
        # Note that you need to have Katydid configured as a bash executable for this to
        # work (as is standard).
        t_start = time.process_time()
        run_katydid = sp.run(
            ["/data/eliza4/he6_cres/katydid/build/bin/Katydid", "-c", config_path],
            capture_output=True,
        )

        print("Katydid output:", run_katydid.stdout[-100:])
        t_stop = time.process_time()

        print(
            "\nfile {}.\ntime to run: {:.2f} s.\ncurrent time: {}.\nroot file created {}\n".format(
                file_df["file_id"],
                t_stop - t_start,
                get_pst_time(),
                file_df["root_file_path"],
            )
        )

        # Delete the copy of the katydid config file once done with processing.
        Path(config_path).unlink()

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
                path.unlink()

        # Force a write to the log.
        sys.stdout.flush()

        return None


if __name__ == "__main__":
    main()
