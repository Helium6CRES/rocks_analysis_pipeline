#!/usr/bin/env python3
import os
import sys
import time
import argparse
import pandas as pd

import pandas.io.sql as psql

import pytz
import numpy as np
import datetime
from glob import glob
import subprocess as sp
import shutil
from shutil import copyfile
import psycopg2
from psycopg2 import Error
import typing
from typing import List

from pathlib import Path
import yaml
import uproot

import numpy as np


# Local imports.
from rocks_utility import (
    get_pst_time,
    set_permissions,
    check_if_exists,
    log_file_break,
)

# Import options.
pd.set_option("display.max_columns", 100)
pd.options.mode.chained_assignment = None  # Comment out if debugging.

def main():
    """
    Args: (from command line)
        run_names (List[str]): all run_names to be post processed. There needs
            to already be root files for these run_names in the associated
            analysis directory.
        analysis_id (int): analysis_id for which to collect track and event
            data for.
        ...

    TODOS:
    * This will only work with katydid files that have track/event objects in the trees.

    """

    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg( "-r", "--run_names", nargs="+", type=str, help="list of runids to collect track data for.",)
    arg( "-a", "--analysis_id", type=int, help="analysis_id to collect track data for.",)
    arg( "-e", "--experiment_name", type=str, help="name used to write the experiment to disk.",)

    args = par.parse_args()

    print( f"\nPost Processing STARTING at PST time: {get_pst_time()}\n")
    # Print summary of experiment:
    print( f"Processing: \n  run_names: {args.run_names}, analysis_id: {args.analysis_id}\n")

    # Force a write to the log. Should add a time out here? How to do that in python cleanly...
    sys.stdout.flush()

    # Deal with permissions (chmod 774, group he6_cres).
    # Done at the beginning and end of main.
    #set_permissions()

    # Step 0: Build the directory structure out for the experiment results and write the root_file_df to it.

    post_processing = PostProcessing(
        args.run_names,
        args.analysis_id,
        args.experiment_name,
    )

    # Done at the beginning and end of main.
    #set_permissions()

    # Current time to nearest second.
    now = datetime.datetime.now().replace(microsecond=0)
    print(f"\nPost Processing Stage DONE at PST time: {get_pst_time()}\n")
    log_file_break()

    return None


class PostProcessing:
    def __init__(
        self,
        run_names,
        analysis_id,
        experiment_name,
    ):

        self.run_names = run_names
        self.analysis_id = analysis_id
        self.experiment_name = experiment_name

        self.machine_path = Path("/data/raid2/eliza4/he6_cres")
        #self.machine_path = Path("/Users/buzinsky/Builds/fake_wulf")

        self.analysis_dir = self.get_analysis_dir()
        self.root_files_df_path = self.analysis_dir / Path(f"root_files.csv")
        self.tracks_df_path = self.analysis_dir / Path(f"tracks.csv")
        #self.track_points_df_path = self.analysis_dir / Path(f"track_points.csv")

        print(f"PostProcessing instance attributes:\n")
        for key, value in self.__dict__.items():
            print(f"{key}: {value}")

        print("constructor")
        print("\nPostProcessing stage 0: set-up.\n")
        self.build_analysis_dir()
        self.root_files_df = self.get_experiment_files()
        self.root_files_df.to_csv(self.root_files_df_path)

        print("\nPostProcessing stage 1: root -> csvs.")

        self.root_files_df = self.load_root_files_df()
        self.process_tracks_and_events()

        print("\nPostProcessing stage 1.5: merge MC truth csvs")
        for run_name in run_names:
            #returns lists of bands.csv's, dmtracks.csvs
            mc_truth_csv_paths = self.get_mc_truth_csv_paths(run_name)
            self.write_mc_truth_csvs(mc_truth_csv_paths[0],"bands.csv")
            self.write_mc_truth_csvs(mc_truth_csv_paths[1],"dmtracks.csv")

        print("\nPostProcessing stage 2: clean-up.")
        self.sanity_check()

        return None

    def get_mc_truth_csv_paths(self, run_name):
        local_path = Path("simulation/sim_results/runs")
        sim_res_dir = self.machine_path / local_path / Path( f"{run_name}")
        bands_csvs = glob(str(sim_res_dir) + "/subrun_*/*/bands.csv")
        dmtracks_csvs = glob(str(sim_res_dir) + "/subrun_*/*/dmtracks.csv")
        print("Found ", len(bands_csvs), " bands.csv's!")
        print("Found ", len(dmtracks_csvs), " dmtracks.csv's!")

        return bands_csvs, dmtracks_csvs

    def write_mc_truth_csvs(self, input_filenames, output_filename):
        #for csv_file_list in
        file_df_list = []

        for input_filename in input_filenames:
            print("ifn", input_filename)
            file_df = pd.read_csv(input_filename)
            file_df["root_file_path"] = input_filename
            file_df_list.append(file_df)

        combined_df = pd.concat(file_df_list).reset_index(drop=True)
        print(combined_df)

        #writes to analysis dir. Probably fine...
        write_path = self.analysis_dir / Path(output_filename)
        combined_df.to_csv(write_path)

        return None

    def get_analysis_dir(self):
        local_path = Path("spec_sims_analysis/saved_experiments")
        analysis_dir = self.machine_path / local_path / Path( f"{self.experiment_name}_a_{self.analysis_id}")
        return analysis_dir

    def build_analysis_dir(self):
        if self.analysis_dir.exists():
            print( f":Current experiment directory: {self.analysis_dir} exists already, it's cool though)")
            #shutil.rmtree(str(self.analysis_dir))
        else:
            self.analysis_dir.mkdir()
            print(f"\nMade: {self.analysis_dir}")

        return None

    def get_experiment_files(self):
        file_df_list = []
        missing_flags = []
        for run_name in self.run_names:
            file_df_path = self.build_file_df_path(run_name)
            file_df = pd.read_csv(file_df_path)
            ### path replacement when doing local tests
            #file_df["root_file_path"] = file_df["root_file_path"].apply(lambda p: self.machine_path / Path(p).relative_to("/data/raid2/eliza4/he6_cres") )
            file_df["root_file_exists"] = file_df["root_file_path"].apply(check_if_exists)
            file_df_list.append(file_df)

        root_files_df = pd.concat(file_df_list).reset_index(drop=True)

        return root_files_df

    def build_file_df_path(self, run_name):
        local_path = Path("spec_sims_analysis/root_files")
        rid_ai_dir = ( self.machine_path / local_path / Path(f"r_{run_name}") / Path(f"aid_{self.analysis_id}"))

        file_df_path = rid_ai_dir / Path( f"rid_df_{run_name}_{self.analysis_id}.csv")
        print("Root file path: ", file_df_path )
        subrun_csvs = glob(str(rid_ai_dir) + f"rid_df_{run_name}_s*_{self.analysis_id}.csv")

        if file_df_path.exists():
            return file_df_path
        elif len(subrun_csvs):
            # if the single rid_df csv does not exist, but the subrun csv's do (from parallel katydid jobs)
            #then merge them into the single csv, just use that
            csv_df_list = [ pd.read_csv(sr_csv) for sr_csv in subrun_csvs]
            combined_df = pd.concat(csv_df_list).reset_index(drop=True)
            print(combined_df)
            combined_df.to_csv(file_df_path)
            return file_df_path
        else:
            raise FileNotFoundError( f"No root file df found for run_name={run_name}, aid={self.analysis_id}")

    def load_root_files_df(self):
        return pd.read_csv(self.root_files_df_path, index_col=0)

    def process_tracks_and_events(self):
        tracks = self.get_track_data_from_files(self.root_files_df)
        #track_points = self.get_track_points_data_from_files(root_files_df)

        processed_tracks = tracks
        print(processed_tracks)

        write_path = self.analysis_dir / Path(f"tracks.csv")
        processed_tracks.to_csv(write_path)

        # Force a write to the log.
        sys.stdout.flush()

        return None

    def get_track_data_from_files(self, root_files_df):

        condition = root_files_df["root_file_exists"] == True

        experiment_tracks_list = [
            self.build_bulk_track_params_for_single_file(root_files_df_row)
            for index, root_files_df_row in root_files_df[condition].iterrows()
        ]

        tracks_df = pd.concat(experiment_tracks_list, axis=0).reset_index(drop=True)

        return tracks_df

#    def get_track_points_data_from_files(self, root_files_df):
#
#        condition = root_files_df["root_file_exists"] == True
#
#        experiment_track_points_list = [
#            self.build_track_points_for_single_file(root_files_df_row)
#            for index, root_files_df_row in root_files_df[condition].iterrows()
#        ]
#
#        track_points_df = pd.concat(experiment_track_points_list, axis=0).reset_index(drop=True)
#
#        return track_points_df


    def build_bulk_track_params_for_single_file(self, root_files_df_row):
        """
        DOCUMENT.
        """

        tracks_df = pd.DataFrame()

        rootfile = uproot.open(root_files_df_row["root_file_path"])

        if "MB-events;1" in rootfile.keys():
            tracks_root = rootfile["MB-events;1"]["MultiBandEvent"]["fTracks"]
            cols = {}
            for key, branch in tracks_root.items():
                # Skip object/pointer branches that trigger the “arbitrary pointer” error
                if branch.interpretation.__class__.__name__ == "AsObjects":
                    continue

                tracks_df[key[9:]] = self.flat(branch.array())

            if cols:
                tracks_df = pd.DataFrame(cols)

        tracks_df["run_name"] = root_files_df_row["run_name"]
        tracks_df["file_id"] = root_files_df_row["file_id"]
        tracks_df["root_file_path"] = root_files_df_row["root_file_path"]
        tracks_df["field"] = root_files_df_row["true_field"]

        return tracks_df.reset_index(drop=True)

    def build_track_points_for_single_file(self, root_files_df_row):
        """
        DOCUMENT.
        """

        track_points_df = pd.DataFrame()

        rootfile = uproot.open(root_files_df_row["root_file_path"])

        if "tracks;1" in rootfile.keys():
            track_points_root = rootfile["tracks;1"]["Track"]["fPoints"]
            cols = {}
            for key, branch in track_points_root.items():
                # Skip object/pointer branches that trigger the “arbitrary pointer” error
                if branch.interpretation.__class__.__name__ == "AsObjects":
                    continue

                track_points_df[key[9:]] = self.flat(branch.array())

            if cols:
                track_points_df = pd.DataFrame(cols)

        track_points_df["run_name"] = root_files_df_row["run_name"]
        track_points_df["file_id"] = root_files_df_row["file_id"]
        track_points_df["root_file_path"] = root_files_df_row["root_file_path"]
        track_points_df["field"] = root_files_df_row["field"]

        return track_points_df.reset_index(drop=True)

    def sanity_check(self):
        desired_path_list = [
            self.root_files_df_path,
            self.tracks_df_path,
            #self.track_points_df_path,
        ]
        real_path_list = self.analysis_dir.glob("*.csv")
        remove_list = list(set(real_path_list) - set(desired_path_list))

        if len(remove_list) == 0:
            print(f"\nSanity check passed. Only files in the analysis dir are: \n")
            for path in desired_path_list:
                print(str(path))
        else:
            print("\nWARNING. sanity_check() failed! ")
            print("Cleaning up. Removing the following files: \n")
            for path in remove_list:
                print(str(path))
                #path.unlink()

        # Force a write to the log.
        sys.stdout.flush()

        return None

    def flat(self, jaggedarray) -> np.ndarray:
        """
        Given jagged array (common in root), it returns a flattened array.
        """
        flatarray = np.array([])
        for a in jaggedarray.tolist():
            flatarray = np.append(flatarray, a)

        return flatarray

if __name__ == "__main__":
    main()
