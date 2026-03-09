#!/usr/bin/env python3
"""
Perform all the preprocessing for a run_id before submitting sbatch for each job
"""
import argparse
import pandas as pd
# import pandas.io.sql as psql

# import psycopg2
# from psycopg2 import Error
# import typing
from typing import List
from pathlib import Path
import sys
import json

# Local imports.
sys.path.append("/data/raid2/eliza4/he6_cres/simulation/he6-cres-spec-sims/src")
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
    CLI entry point. Not currently used: launch_katydid imports KatydidPreprocessing directly. Might delete later, but keeping for consistency for now. 
    """

    par = argparse.ArgumentParser()
    arg = par.add_argument()
    arg("-id", "--run_id", type=int, 
        help="run_id to run katydid on")
    arg("-nid", "--noise_run_id", type=int, 
        help="run_id to use for noise floor in katydid run. If -1 then will use self as noise file.")
    arg("-aid", "--analysis_id", type=int,
        help="analysis_id used to label directories.")
    arg("-b", "--base_config", type=str,
        help="base .yaml katydid config file to be run on run_id, should exist in base config directory.")
    arg("-fn", "--file_num", default=-1, type=int,
        help="Number of files in run_id to analyzie (<= number of files in run_id)")
    arg("--aid_passed", action="store_true",
        help="Flag to indicate that the user specified aid explicitly, instead the default value. If so, will perform a cleanup if the aid exists or run as normal.")

    args = par.parse_args()

    KatydidPreprocessing(
        args.run_id,
        args.analysis_id,
        args.noise_run_id,
        args.base_config,
        args.file_num,
        args.aid_passed,
    )


class KatydidPreprocessing:
    def __init__(self, run_id, analysis_id, noise_run_id, base_config, file_num, aid_passed=False):

        self.run_id = run_id
        self.analysis_id = analysis_id
        self.noise_run_id = noise_run_id
        self.base_config = base_config
        self.file_num = file_num
        self.aid_passed = aid_passed

        print(f"\nRunning Katydid preprocessing. STARTING at PST time: {get_pst_time()}\n")
        print(f"\nPreprocessing: run_id: {run_id}.\n")

        # Force a write to the log.
        sys.stdout.flush()

        # appropriate access.
        set_permissions()

        # Print run summary.
        self.print_run_summary()

        # Build the path to the file_df.
        self.build_file_df_path()

        # Collect the file_df. This means deciding if cleanup or new analysis.
        self.file_df = self.collect_file_df()

        # set_permissions()

        print(f"\nRunning Katydid preprocessing on {run_id} DONE at PST time: {get_pst_time()}\n")
        log_file_break()

    def print_run_summary(self):
        print("\nRun Summary:")
        print(f"run_id: {self.run_id}")
        print(f"analysis_id: {self.analysis_id}")
        print(f"base_config: {self.base_config}\n")
        return None


    def build_file_df_path(self):
        """
        Build paths to directories in katydid_analysis/root_files and csvs in those directories with file information.
        """
        base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/root_files")
        rid_ai_dir = (
            base_path
            / Path(f"rid_{self.run_id:04d}")
            / Path(f"aid_{self.analysis_id:03d}")
        )

        self.file_df_path = rid_ai_dir / Path(
            f"rid_df_{self.run_id:04d}_{self.analysis_id:03d}.csv"
        )
        self.file_df_json_path = rid_ai_dir / Path(
            f"rid_df_{self.run_id:04d}_{self.analysis_id:03d}.json"
        )

        self.is_cleanup = self.aid_passed and self.file_df_path.is_file() and self.file_df_json_path.is_file()


    def collect_file_df(self):
        # This function figures out if the run is a clean up or new analysis
        # and collects the file_df.
        # This entails determining if this should be a clean-up or new analysis.
        # Clean up if file_df csv exists and the aid was specified by the user as a command line argument.

        if self.is_cleanup:
            print(
                f"Analysis Type: Clean up. \nfile_df {self.file_df_path} already exists.\n"
            )

            file_df = pd.read_csv(self.file_df_path)
            file_df["rocks_file_path"] = file_df["rocks_file_path"].apply(json.loads)
            file_df["rocks_noise_file_path"] = file_df["rocks_noise_file_path"].apply(json.loads)

            if self.get_base_config_path() != file_df["base_config_path"]:
                raise ValueError(
                        f"Trying to run cleanup using config at {self.get_base_config_path()}, but original run used {file_df['base_config_path']}. Rerun using the same config or run as new analysis."
                        )

            # The following is a sanity check to make sure the number of files in the clean-up
            # match the number of files that were originally run. Then trim the df according
            # to the file_num arg.

            if self.file_num != len(file_df):
                print(
                    f"Warning: The file_num specified in this cleanup \
                    doesn't match the file_num originally run with ({len(file_df)}).\
                    Trimming to match current file_num ({self.file_num})"
                )
                file_df = file_df[: self.file_num]

            # Check to see which root files already exist.
            file_df["root_file_exists"] = file_df["root_file_path"].apply(check_if_exists)

        # New analysis if the file does not exist or the aid was not specified.
        else:
            print("Analysis Type: New analysis. \nBuilding file_df.\n")
            file_df = self.build_full_file_df()
        return file_df

    def build_full_file_df(self):
        """
        Populate df of spec(k) files with metadata and path information. 
        """

        file_df = self.create_base_file_df(self.run_id)
        file_df["analysis_id"] = self.analysis_id
        file_df["root_file_exists"] = False
        file_df["file_id"] = file_df.index
        file_df["rocks_file_path"] = file_df["file_path"].apply(self.process_fp)
        file_df["exists"] = file_df["rocks_file_path"].apply(check_if_exists)

        file_df["approx_slope"] = self.get_slope(file_df["true_field"][0])

        dbscan_r = self.get_dbscan_radius(file_df["approx_slope"][0])
        file_df["dbscan_radius_0"] = dbscan_r[0]
        file_df["dbscan_radius_1"] = dbscan_r[1]

        file_df["base_config_path"] = self.get_base_config_path()
        file_df["output_dir"] = self.build_dir_structure()

        # Collect either the given noise id or assign 'self' to noise file path.
        if self.noise_run_id == -1:
            print("\nUsing 'self' as noise file in katydid analysis.\n")
            file_df["noise_file_path"] = file_df["file_path"]
        else:
            noise_fp_list = self.get_noise_fp()
            file_df["noise_file_path"] = [noise_fp_list] * len(file_df)

        file_df["rocks_noise_file_path"] = file_df["noise_file_path"].apply(self.process_fp)

        file_df["root_file_path"] = file_df.apply(
            lambda row: self.build_root_file_path(row), axis=1
        )

        file_df["slew_file_path"] = file_df.apply(
            lambda row: self.build_slew_file_path(row), axis=1
        )

        # Trim the df according to the file_num arg.
        if self.file_num != -1:
            file_df = file_df[: self.file_num]

        # Before running katydid write this df to the analysis dir.
        # This will be used during the cleanup run.
        print(f"Built file_df: {self.file_df_path}")

        file_df.to_json(self.file_df_json_path, index=False)
        # Copy file_df before writing to csv so we don't mutate the in-memory df we return
        file_df_csv = file_df.copy()
        file_df_csv["rocks_file_path"] = file_df_csv["rocks_file_path"].map(json.dumps)
        file_df_csv["rocks_noise_file_path"] = file_df_csv["rocks_noise_file_path"].map(json.dumps)
        file_df_csv.to_csv(self.file_df_path, index=False)

        return file_df

    def create_base_file_df(self, run_id: int):
        # DOCUMENT.
        """
        Query He6-CRES db for spec(k) files corresponding to a given rid, construct a dataframe of spec(k) files grouped by file_id.
        """
        query_he6_db = """
                        SELECT r.run_id, f.spec_id, f.file_in_acq, f.channel, f.file_path, r.true_field, r.set_field
                        FROM he6cres_runs.run_log as r
                        RIGHT JOIN he6cres_runs.spec_files as f
                        ON r.run_id = f.run_id
                        WHERE r.run_id = {}
                        ORDER BY r.created_at DESC
                      """.format(
            run_id
        )

        file_df = he6cres_db_query(query_he6_db)

        # print(file_df['true_field'])

        # need to check that true_field was filled and is not NAN. If NAN, check database and 
        all_nan_true_field = file_df['true_field'].isna().all()
        print(f"All values in column 'true_field' are NaN: {all_nan_true_field}")
        if all_nan_true_field:
            file_df['true_field'] = file_df['set_field'].abs()

        # print(file_df['true_field'])

        # Group by file_inAcq and apply the aggregation function
        file_df = file_df.groupby('file_in_acq').apply(self.aggregate_paths).reset_index(drop=True)
        return file_df

    # Define a function to aggregate file_path into a list ordered by channel
    def aggregate_paths(self, group):
        ordered_paths = group.sort_values(by='channel')['file_path'].apply(str).tolist()
        return pd.Series({
            'run_id': group['run_id'].iloc[0],
            'true_field': group['true_field'].iloc[0],
            'file_path': ordered_paths
        })

    def process_fp(self, daq_fp_list):
        #print(daq_fp_list)
        rocks_fp_list = ["/data/raid2/eliza4/he6_cres/" + daq_fp[5:] for daq_fp in daq_fp_list]
        return rocks_fp_list

    def get_slope(self, true_field, frequency: float = 19.15e9):

        approx_power = sc.power_larmor(true_field, frequency)
        approx_energy = sc.freq_to_energy(frequency, true_field)
        approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

        return approx_slope

    def get_dbscan_radius(
        self, approx_slope: float, dbscan_base_radius: List[float] = [5.0e-4, 40e6]
    ) -> List[float]:
        """
        This does work. I just checked the math. Use the fact that dbscan_base_radius[1]/dbscan_base_radius[0]
        = base_slope.
        """

        dbscan_radius = [
            dbscan_base_radius[1] / approx_slope,
            dbscan_base_radius[0] * approx_slope,
        ]

        return dbscan_radius

    def get_base_config_path(self):

        base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/base_configs")
        base_config_full = base_path / Path(self.base_config)

        if not base_config_full.is_file():
            raise UserWarning("base config doesn't exist. ")

        return str(base_config_full)

    def build_dir_structure(self):

        base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/root_files")

        run_id_dir = base_path / Path(f"rid_{self.run_id:04d}")

        if not run_id_dir.is_dir():
            raise UserWarning("This directory should have been made already.")

        current_analysis_dir = run_id_dir / Path(f"aid_{self.analysis_id:03d}")
        if not current_analysis_dir.is_dir():
            current_analysis_dir.mkdir(exist_ok=True) # guard against race condition
            print(f"Created directory: {current_analysis_dir}")

        return str(current_analysis_dir)

    def get_noise_fp(self):
        """
        DOCUMENT
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

        return noise_file_path

    def build_root_file_path(self, file_df):
        root_path = Path(file_df["output_dir"]) / str(
            Path(file_df["rocks_file_path"][0]).stem[:-2] + file_df["output_dir"][-4:] + ".root"
        )

        return str(root_path)

    def build_slew_file_path(self, file_df):
        slew_path = Path(file_df["output_dir"]) / str(
            Path(file_df["rocks_file_path"][0]).stem[:-2] + file_df["output_dir"][-4:] + "_SlewTimes.txt"
        )

        return str(slew_path)


