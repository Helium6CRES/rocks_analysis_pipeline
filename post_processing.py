#!/usr/bin/env python3
import os
import sys
import time
import argparse
import pandas as pd
import datetime
from glob import glob
import subprocess as sp
import shutil
from shutil import copyfile
import psycopg2
from psycopg2 import Error
import typing
from typing import List
import pandas.io.sql as psql
from pathlib import Path
import yaml
import uproot4
import awkward
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc

# Import options.
pd.set_option("display.max_columns", 100)


def main():
    """

    TODOS:
    * Build this into a class. It's going to be much easier to read and interact with.
    * I need to build the clean-up and event-building into this process. Otherwise these
    files are going to get too large. Already 1.1G after 171 out of 5700 files.
    * Make sure that the files with no tracks are still getting kept track of somehow. Maybe just in the file df?
    * Put a timestamp in the log files for clean-up.
    * Put a timestamp in the dfs somehow so we know when the analysis was conducted.
    * Get Sphynx working before moving on to documenting! This will be so useful.
    * Make an option to delete a currently existing experiment directory with a user enter.


    Notes:
    * This will only work with katydid files that have track/event objects in the trees.
    * Should it just take a analysis id? No then it isn't well defined...
    """

    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg(
        "-rids",
        "--run_ids",
        nargs="+",
        type=int,
        help="list of runids to collect track data for.",
    )
    arg(
        "-aid",
        "--analysis_id",
        type=int,
        help="analysis_id to collect track data for.",
    )

    arg(
        "-name",
        "--experiment_name",
        type=str,
        help="name used to write the experiment to disk.",
    )

    arg(
        "-nft",
        "--num_files_tracks",
        type=int,
        help="number of files for which to save track data per run_id.",
    )

    arg(
        "-nfe",
        "--num_files_events",
        type=int,
        help="number of files for which to save cleaned-up event data per run_id.",
    )

    args = par.parse_args()

    # Print summary of experiment:
    print(
        f"Experiment Summary\n run_ids: {args.run_ids}, analysis_id: {args.analysis_id}\n"
    )

    # Force a write to the log.
    sys.stdout.flush()

    # Deal with permissions (chmod 770, group he6_cres).
    # Done at the beginning and end of main.
    set_permissions()

    post_processing = PostProcessing(
        args.run_ids,
        args.analysis_id,
        args.experiment_name,
        args.num_files_tracks,
        args.num_files_events,
    )

    # NEXT (9/12/22):
    # * Work on getting the nft, nfe working.
    # * Build the cleaning method out. And the writing of the events to disk.
    #   * Make the defualt of that -1 meaning all of them and the default for nft to be 1 or something?
    # * keep it moving.
    # * Work on visualziation stuff on the local machine.
    # * Get the utility functions like the database call into another module for cleanliness.
    # *
    print("STOP NOW.")

    # analysis_id = args.analysis_id
    # run_ids = args.run_ids
    # experiment_name = args.experiment_name

    # analysis_dir = build_analysis_dir(experiment_name, analysis_id)

    # file_df_experiment = get_experiment_files(run_ids, analysis_id)

    # # TODO: Deal with file_num vs file_id
    # # TODO: Build files.csv, tracks.csv.

    # condition = file_df_experiment["root_file_exists"] == True
    # print("Fraction of root files;", condition.mean())

    # # file_df_experiment[condition].apply(lambda row: sanity_check(row), axis = 1)

    # print(len(file_df_experiment))
    # print(file_df_experiment.columns)
    # write_files_df(file_df_experiment, analysis_dir)

    # # Go through 50 files at a time.
    # n = 50  # chunk row size
    # list_file_df = [
    #     file_df_experiment[i : i + n] for i in range(0, file_df_experiment.shape[0], n)
    # ]

    # for chunk_idx, file_df_chunk in enumerate(list_file_df):
    #     print(len(file_df_chunk))
    #     tracks_df_chunk = get_experiment_tracks(file_df_chunk)

    #     write_tracks_df(chunk_idx, tracks_df_chunk, analysis_dir)

    return None


class PostProcessing:
    def __init__(
        self, run_ids, analysis_id, experiment_name, num_files_tracks, num_files_events
    ):

        self.run_ids = run_ids
        self.analysis_id = analysis_id
        self.experiment_name = experiment_name
        self.num_files_tracks = num_files_tracks
        self.num_files_events = num_files_events

        self.analysis_dir = self.build_analysis_dir()
        self.root_files_df = self.get_experiment_files()

        # TODO: Delete once done.
        print(self.root_files_df.head(1).to_string())
        print(self.root_files_df.index)

        # Now gather tracks, clean them up, write some of them to disk, and write events to disk.
        self.process_tracks_and_events()

    def build_analysis_dir(self):

        base_path = Path("/data/eliza4/he6_cres/katydid_analysis/saved_experiments")

        analysis_dir = base_path / Path(f"{self.experiment_name}_aid_{self.analysis_id}")

        if analysis_dir.exists():
            input(
                f"CAREFUL!! Press enter to delete and rebuild the following directory:\n{analysis_dir}"
            )
            shutil.rmtree(str(analysis_dir))

        analysis_dir.mkdir()
        print(f"Made {analysis_dir}")

        return analysis_dir

    def get_experiment_files(self):

        # Step 0: Make sure that all of the listed rids/aid exists.
        file_df_list = []
        for run_id in self.run_ids:
            file_df_path = self.build_file_df_path(run_id)

            if file_df_path.is_file():
                print(f"Collecting file_df: {str(file_df_path)} \n")

                file_df = pd.read_csv(file_df_path)
                file_df["root_file_exists"] = file_df["root_file_path"].apply(
                    lambda x: check_if_exists(x)
                )
                file_df_list.append(file_df)

            # This file_df should already exist.
            else:
                raise UserWarning(
                    f"run_id {run_id} has no analysis_id {self.analysis_id}"
                )

        root_files_df = pd.concat(file_df_list).reset_index(drop=True)

        self.write_to_csv(0, root_files_df, file_name = "root_files")

        return root_files_df

    def build_file_df_path(self, run_id):

        base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")
        rid_ai_dir = (
            base_path / Path(f"rid_{run_id:04d}") / Path(f"aid_{self.analysis_id:03d}")
        )

        file_df_path = rid_ai_dir / Path(
            f"rid_df_{run_id:04d}_{self.analysis_id:03d}.csv"
        )

        return file_df_path

    def process_tracks_and_events(self):

        if self.num_files_events < self.num_files_tracks:
            raise ValueError("num_files_events must be >= than num_files_tracks.")

        # We groupby file_id so that we can write a different number of tracks and events
        # worth of root files to disk for each run_id.
        for file_id, root_files_df_chunk in self.root_files_df.groupby(["file_id"]):

            tracks = self.get_track_data_from_files(root_files_df_chunk)

            # Write out tracks to csv for first nft file_ids (command line argument).
            if file_id < self.num_files_tracks:

                self.write_to_csv(file_id, tracks, file_name="tracks")

            print(f"file_id: {file_id}")
            print(len(root_files_df_chunk))
            print(len(tracks))
            print(tracks.index)
            print(tracks.head())

            # Write out events to csv for first nfe file_ids (command line argument).
            if file_id < self.num_files_events:

                events = self.get_event_data_from_tracks(tracks)
                self.write_to_csv(file_id, events, file_name="events")

            else:
                break

            # START HERE. Figure out cleaning and event reconstruction.
            # Keep it neat and clean.
            # SHOULD I MAKE THIS ABLE TO RUN ON MULTIPLE NODES??

    def get_event_data_from_tracks(self, tracks):

        # Step 0. Clean up the tracks.
        cleaned_tracks = self.clean_up_tracks(tracks)

        print("cleaned_tracks: ", cleaned_tracks.index, cleaned_tracks.head())

        # Step 1. Add aggregated event info to tracks.
        tracks = self.add_event_info(tracks)
        print("1\n", tracks.index)

        # Step 2. DBSCAN clustering of events.
        # TODO: Make sure it actually 
        tracks = self.cluster_tracks(tracks)
        print("3\n", tracks.index)

        # Step 3. Build event df.
        events = self.build_events(tracks)
        print("3\n", events.index)

        return events

    def get_track_data_from_files(self, root_files_df):

        # TODO: Wait how to organize this? Because I don't want to have to call this again when doing the cleaning...
        # TODO: Get it to all work for a

        condition = root_files_df["root_file_exists"] == True

        experiment_tracks_list = [
            build_tracks_for_single_file(root_file_path, run_id, file_id)
            for root_file_path, run_id, file_id in zip(
                root_files_df[condition]["root_file_path"],
                root_files_df[condition]["run_id"],
                root_files_df[condition]["file_id"],
            )
        ]

        tracks_df = pd.concat(experiment_tracks_list, axis=0).reset_index(drop=True)

        tracks_df = self.add_track_info(tracks_df)

        return tracks_df

    def build_tracks_for_single_file(self, root_file_path, run_id, file_id):
        """
        DOCUMENT.
        """

        tracks_df = pd.DataFrame()

        rootfile = uproot4.open(root_file_path)

        if "multiTrackEvents;1" in rootfile.keys():

            tracks_root = rootfile["multiTrackEvents;1"]["Event"]["fTracks"]

            for key, value in tracks_root.items():
                # Slice the key so it drops the redundant "fTracks."
                tracks_df[key[9:]] = self.flat(value.array())

        tracks_df["run_id"] = run_id
        tracks_df["file_id"] = file_id
        tracks_df["root_file_path"] = root_file_path

        tracks_df = self.add_env_data(tracks_df)

        return tracks_df

    def add_track_info(self, tracks):

        tracks["FreqIntc"] = (
            tracks["EndFrequency"] - tracks["EndTimeInRunC"] * tracks["Slope"]
        )
        tracks["TimeIntc"] = (
            tracks["StartTimeInRunC"] - tracks["StartFrequency"] / tracks["Slope"]
        )

        intc_info = (
            tracks.groupby(["run_id", "file_id", "EventID"])
            .agg(
                TimeIntc_mean=("TimeIntc", "mean"),
                TimeIntc_std=("TimeIntc", "std"),
                TimeLength_mean=("TimeLength", "mean"),
                TimeLength_std=("TimeLength", "std"),
                Slope_mean=("Slope", "mean"),
                Slope_std=("Slope", "std"),
            )
            .reset_index()
        )

        tracks = pd.merge(
            tracks, intc_info, how="left", on=["run_id", "file_id", "EventID"]
        )

        return tracks

    def clean_up_tracks(
        self, tracks, cols=["TimeIntc", "TimeLength", "Slope"], cut_levels=[2, 2, 2]
    ):

        cut_condition = self.create_track_cleaning_cut(tracks, cols, cut_levels)

        tracks = tracks[cut_condition]

        return tracks

    def create_track_cleaning_cut(self, tracks, cols, cut_levels):

        # cols_mean = [col + "mean" for col in cols]
        # cols_std = [col + "std" for col in cols]
        conditions = [
            (
                (np.abs((tracks[col] - tracks[col + "_mean"]) / tracks[col + "_std"]))
                < cut_level
            )
            for col, cut_level in zip(cols, cut_levels)
        ]

        # Hacky way to and the list of boolean conditions. Couldn't figure out how to vectorize it.
        condition_tot = np.ones_like(conditions[0])
        for condition in conditions:
            condition_tot = condition_tot & condition

        return condition_tot

    def dbscan_clustering(self, df, features: list, eps: float, min_samples: int):

        # Normalize features.
        X_norm = StandardScaler().fit_transform(df[features])
        # Compute DBSCAN
        db = DBSCAN(eps=eps, min_samples=min_samples).fit(X_norm)
        labels = db.labels_

        return labels

    def cluster_tracks(
        self, tracks, eps=0.003, min_samples=1, features=["EventTimeIntc"]
    ):

        exp_tracks_copy = tracks.copy()
        exp_tracks_copy["event_label"] = 100

        for i, (name, group) in enumerate(
            exp_tracks_copy.groupby(["run_id", "file_id"])
        ):

            print(f"\n clustering: run_id: {name[0]},  file_id: {name[1]}")

            condition = (exp_tracks_copy.run_id == name[0]) & (
                exp_tracks_copy.file_id == name[1]
            )
            print(f"tracks in file: {condition.sum()}")
            print(f"fraction of total requested (nfe files): {condition.mean()}")

            exp_tracks_copy.loc[condition, "event_label"] = self.dbscan_clustering(
                exp_tracks_copy[condition],
                features=list(features),
                eps=eps,
                min_samples=min_samples,
            )
        exp_tracks_copy["EventID"] = exp_tracks_copy["event_label"] + 1

        return exp_tracks_copy

    def add_event_info(self, tracks_in: pd.DataFrame) -> pd.DataFrame:

        tracks = tracks_in.copy()

        tracks["MeanTrackSNR"] = tracks["TotalTrackSNR"] / tracks["NTrackBins"]

        tracks["FreqIntc"] = (
            tracks["EndFrequency"] - tracks["EndTimeInRunC"] * tracks["Slope"]
        )
        tracks["TimeIntc"] = (
            tracks["StartTimeInRunC"] - tracks["StartFrequency"] / tracks["Slope"]
        )

        tracks["EventStartTime"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "StartTimeInRunC"
        ].transform("min")
        tracks["EventEndTime"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "EndTimeInRunC"
        ].transform("max")

        tracks["EventStartFreq"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "StartFrequency"
        ].transform("min")
        tracks["EventEndFreq"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "EndFrequency"
        ].transform("max")

        tracks["EventTimeLength"] = tracks["EventEndTime"] - tracks["EventStartTime"]
        tracks["EventFreqLength"] = tracks["EventEndFreq"] - tracks["EventStartFreq"]
        tracks["EventNBins"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "NTrackBins"
        ].transform("sum")
        tracks["EventSlope"] = tracks["EventFreqLength"] / tracks["EventTimeLength"]

        tracks["EventTrackCoverage"] = (
            tracks.groupby(["run_id", "file_id", "EventID"])["TimeLength"].transform(
                "sum"
            )
            / tracks["EventTimeLength"]
        )

        tracks["EventMeanSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MeanTrackSNR"
        ].transform("mean")

        tracks["EventTrackTot"] = tracks.groupby(
            ["run_id", "file_id", "EventID"]
        ).EventSequenceID.transform("count")

        tracks["EventFreqIntc"] = (
            tracks["EventEndFreq"] - tracks["EventEndTime"] * tracks["EventSlope"]
        )
        tracks["EventTimeIntc"] = (
            tracks["EventStartTime"] - tracks["EventStartFreq"] / tracks["EventSlope"]
        )

        return tracks

    def build_events(self, tracks: pd.DataFrame) -> pd.DataFrame:

        # tracks = add_event_info(tracks_in)
        event_cols = [
            "run_id",
            "file_id",
            "EventID",
            "EventStartTime",
            "EventEndTime",
            "EventStartFreq",
            "EventEndFreq",
            "EventTimeLength",
            "EventFreqLength",
            "EventTrackCoverage",
            "EventMeanSNR",
            "EventSlope",
            "EventNBins",
            "EventTrackTot",
            "EventFreqIntc",
            "EventTimeIntc",
            "field",
            "monitor_rate",
        ]
        events = (
            tracks.groupby(["run_id", "file_id", "EventID"])
            .first()
            .reset_index()[event_cols]
        )

        return events

    def add_env_data(self, tracks_df):

        # TODO: Fill in this function.
        # maybe import whatever does this from another module? Ehh maybe better to just have it all here.
        tracks_df["field"] = 10
        tracks_df["monitor_rate"] = 10

        return tracks_df

    def write_to_csv(self, file_id, df_chunk, file_name):
        print(f"Writing {file_name} data to disk for file_id {file_id}.")
        write_path = self.analysis_dir / Path(f"{file_name}.csv")

        if file_id == 0:
            df_chunk.to_csv(write_path)
        else:
            # append data frame to existing CSV file.
            df_chunk.to_csv(write_path, mode="a")

        return None

    def flat(self, jaggedarray: awkward.Array) -> np.ndarray:
        """
        Given jagged array (common in root), it returns a flattened array.

        Args:
            jaggedarray (awkward array): No specifications.

        Returns:
            array (np.ndarray): No specifications.

        """
        flatarray = np.array([])
        for a in jaggedarray.tolist():
            flatarray = np.append(flatarray, a)

        return flatarray


# def clean_track_data(self):

#     return None


# def get event_data(self):

# TODO:

# print(len(tracks_df_experiment))
# print(tracks_df_experiment.head().columns)
# print(tracks_df_experiment.head(100).to_string())

# Now build these two things into a an instance of a data class.

# Then pickle the object and put it somewhere.

# Then work on data cleaning and visualization and stuff.


def sanity_check(file_df):
    print("\n")
    print(file_df["run_id"], file_df["file_num"])
    rootfile = uproot4.open(file_df["root_file_path"])

    print(rootfile.keys())
    if "multiTrackEvents;1" in rootfile.keys():
        print("Yes tracks.")
        print("{}".format(rootfile["multiTrackEvents;1"]["Event"]["fTracks"]))
    else:
        print("No tracks.")
    print("\n")

    return None


def write_files_df(file_df_experiment, analysis_dir):

    files_path = analysis_dir / Path("files.csv")

    file_df_experiment.to_csv(files_path)

    return None


def write_tracks_df(chunk_idx, tracks_df_chunk, analysis_dir):

    tracks_path = analysis_dir / Path("tracks.csv")
    if chunk_idx == 0:
        # append data frame to CSV file
        tracks_df_chunk.to_csv(tracks_path)
    else:
        # append data frame to CSV file
        tracks_df_chunk.to_csv(tracks_path, mode="a")

    return None


def build_analysis_dir(experiment_name, analysis_id):

    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/saved_experiments")

    analysis_dir = base_path / Path(f"{experiment_name}_{analysis_id}")

    if analysis_dir.exists():
        raise UserWarning(f"{analysis_dir} already exists.")
    else:
        analysis_dir.mkdir()
        print(f"Made {analysis_dir}")

    return analysis_dir


def get_experiment_files(run_ids, analysis_id):

    # Step 0: Make sure that all of the listed rids/aid exists.
    file_df_list = []
    for run_id in run_ids:
        file_df_path = build_file_df_path(run_id, analysis_id)

        if file_df_path.is_file():
            print(f"Collecting file_df: {str(file_df_path)} \n")

            file_df = pd.read_csv(file_df_path, index_col=0)
            file_df["root_file_exists"] = file_df["root_file_path"].apply(
                lambda x: check_if_exists(x)
            )
            file_df_list.append(file_df)

        # New analysis.
        else:
            raise UserWarning(f"run_id {run_id} has no analysis_id {analysis_id}")

    file_df_experiment = pd.concat(file_df_list)

    return file_df_experiment


def get_experiment_tracks(file_df_experiment):
    # TODO: Change the run_num to file_id (needs to match in the file_df creation!)

    condition = file_df_experiment["root_file_exists"] == True

    experiment_tracks_list = [
        build_tracks_for_single_file(root_file_path, run_id, file_id)
        for root_file_path, run_id, file_id in zip(
            file_df_experiment[condition]["root_file_path"],
            file_df_experiment[condition]["run_id"],
            file_df_experiment[condition]["file_num"],
        )
    ]
    return pd.concat(experiment_tracks_list, axis=0).reset_index(drop=True)


# TODO: MAKE A DATA CLASS?
# TODO: file_num, file_id, file_in_acq. These need to be made consistent.


def build_tracks_for_single_file(root_file_path, run_id, file_id):
    """ """

    tracks_df = pd.DataFrame()

    rootfile = uproot4.open(root_file_path)

    if "multiTrackEvents;1" in rootfile.keys():

        tracks_root = rootfile["multiTrackEvents;1"]["Event"]["fTracks"]

        for key, value in tracks_root.items():
            # Slice the key so it drops the redundant "fTracks."
            tracks_df[key[9:]] = flat(value.array())

    tracks_df["run_id"] = run_id
    tracks_df["file_id"] = file_id
    tracks_df["root_file_path"] = root_file_path

    tracks_df = add_env_data(run_id, file_id, tracks_df)

    return tracks_df


def add_env_data(run_id, file_id, tracks_df):

    # TODO: Fill in this function.
    tracks_df["field"] = 10
    tracks_df["monitor_rate"] = 10

    return tracks_df


# TODO: Duplicate function. Refactor.
def build_file_df_path(run_id, analysis_id):
    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")
    rid_ai_dir = base_path / Path(f"rid_{run_id:04d}") / Path(f"aid_{analysis_id:03d}")

    file_df_path = rid_ai_dir / Path(f"rid_df_{run_id:04d}_{analysis_id:03d}.csv")
    return file_df_path


def set_permissions():

    set_group = sp.run(["chgrp", "-R", "he6_cres", "katydid_analysis/"])
    set_permission = sp.run(["chmod", "-R", "774", "katydid_analysis/"])

    return None


def check_if_exists(fp):
    return Path(fp).is_file()


def flat(jaggedarray: awkward.Array) -> np.ndarray:
    """
    Given jagged array (common in root), it returns a flattened array.

    Args:
        jaggedarray (awkward array): No specifications.

    Returns:
        array (np.ndarray): No specifications.

    """
    flatarray = np.array([])
    for i in jaggedarray.tolist():
        flatarray = np.append(flatarray, i)

    return flatarray


if __name__ == "__main__":
    main()
