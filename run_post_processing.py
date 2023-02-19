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
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

# Local imports.
sys.path.append("/data/eliza4/he6_cres/simulation/he6-cres-spec-sims")
import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc


# Local imports.
from rocks_utility import (
    he6cres_db_query,
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
    Main for the post processing of the katydid output. SAY MORE

    Args: (from command line)
        run_ids (List[int]): all run_ids to be post processed. There needs
            to already be root files for these run_ids in the associated
            analysis directory.
        analysis_id (int): analysis_id for which to collect track and event
            data for.
        ...

    Returns:
        None

    Raises:
        None

    TODOS:
    * This will only work with katydid files that have track/event objects in the trees.

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

    arg(
        "-fid",
        "--file_id",
        type=int,
        help="file_id to be processed. Each file_id (across all run_ids) are sent out to a different node.",
    )
    arg(
        "-stage",
        "--stage",
        type=int,
        help="""0: set-up. The root file df will be made and the results directory will be build.
                1: processing. The tracks and events will be extracted from root files and written 
                    to disk in the results directory. 
                2: clean-up. The many different csvs worth of tracks and events will be combined into 
                    single files. 
            """,
    )

    args = par.parse_args()

    print(
        f"\nPost Processing Stage {args.stage} STARTING at PST time: {get_pst_time()}\n"
    )

    # Print summary of experiment:
    print(
        f"Processing: \n file_id: {args.file_id} run_ids: {args.run_ids}, analysis_id: {args.analysis_id}\n"
    )

    # Force a write to the log.
    sys.stdout.flush()

    # Deal with permissions (chmod 774, group he6_cres).
    # Done at the beginning and end of main.
    set_permissions()

    # Step 0: Build the directory structure out for the experiment results and write the root_file_df to it.

    post_processing = PostProcessing(
        args.run_ids,
        args.analysis_id,
        args.experiment_name,
        args.num_files_tracks,
        args.num_files_events,
        args.file_id,
        args.stage,
    )

    # Done at the beginning and end of main.
    set_permissions()

    # Current time to nearest second.
    now = datetime.datetime.now().replace(microsecond=0)
    print(f"\nPost Processing Stage {args.stage} DONE at PST time: {get_pst_time()}\n")
    log_file_break()

    return None


class PostProcessing:
    def __init__(
        self,
        run_ids,
        analysis_id,
        experiment_name,
        num_files_tracks,
        num_files_events,
        file_id,
        stage,
    ):

        self.run_ids = run_ids
        self.analysis_id = analysis_id
        self.experiment_name = experiment_name
        self.num_files_tracks = num_files_tracks
        self.num_files_events = num_files_events
        self.file_id = file_id
        self.stage = stage

        self.analysis_dir = self.get_analysis_dir()
        self.root_files_df_path = self.analysis_dir / Path(f"root_files.csv")
        self.tracks_df_path = self.analysis_dir / Path(f"tracks.csv")
        self.events_df_path = self.analysis_dir / Path(f"events.csv")

        print(f"PostProcessing instance attributes:\n")
        for key, value in self.__dict__.items():
            print(f"{key}: {value}")

        if self.stage == 0:

            print("\nPostProcessing stage 0: set-up.\n")
            self.build_analysis_dir()
            self.root_files_df = self.get_experiment_files()

            # Add per-file nmr and monitor rate data.
            self.root_files_df = self.add_env_data(self.root_files_df)

            # Write the root_files_df to disk for use in the subsequent stages.
            self.root_files_df.to_csv(self.root_files_df_path)

        elif self.stage == 1:

            print("\nPostProcessing stage 1: cleaning and clustering.")
            # Process all the files with given file_id.

            # Start by opening and reading in the file_df.
            self.root_files_df = self.load_root_files_df()

            # Check to see if the event_i.csv file already exists.
            # If so, then we won't reprocess.
            events_path = self.analysis_dir / Path(f"events_{self.file_id}.csv")
            if events_path.is_file(): 
                print(f"No processing necessary. Events csv already processed: {events_path}")
            
            else: 

                # Now gather tracks, clean them up, build events. Write csvs to disk.
                self.process_tracks_and_events()

        elif self.stage == 2:

            print("\nPostProcessing stage 2: clean-up.")
            # Start by opening and reading in the file_df.
            # self.root_files_df = self.load_root_files_df()

            self.merge_csvs()
            self.sanity_check()

        return None

    def get_analysis_dir(self):

        base_path = Path("/data/eliza4/he6_cres/katydid_analysis/saved_experiments")

        analysis_dir = base_path / Path(
            f"{self.experiment_name}_aid_{self.analysis_id}"
        )
        return analysis_dir

    def build_analysis_dir(self):

        if self.analysis_dir.exists():

            print(
                f"WARNING: Deleting current experiment directory: {self.analysis_dir}"
            )
            shutil.rmtree(str(self.analysis_dir))

        self.analysis_dir.mkdir()
        print(f"\nMade: {self.analysis_dir}")

        return None

    def get_experiment_files(self):

        # Step 0: Make sure that all of the listed rids/aid exists.
        file_df_list = []
        for run_id in self.run_ids:
            file_df_path = self.build_file_df_path(run_id)

            if file_df_path.is_file():

                file_df = pd.read_csv(file_df_path, index_col=0)
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

    def load_root_files_df(self):

        return pd.read_csv(self.root_files_df_path, index_col=0)

    def process_tracks_and_events(self):

        if self.num_files_events < self.num_files_tracks:
            raise ValueError("num_files_events must be >= than num_files_tracks.")

        # We groupby file_id so that we can write a different number of tracks and events
        # worth of root files to disk for each run_id.
        root_files_df_chunk = self.root_files_df[
            self.root_files_df.file_id == self.file_id
        ]
        if len(root_files_df_chunk) == 0:
            raise UserWarning(
                f"There is no file_id = {self.file_id} in aid = {self.analysis_id}"
            )

        tracks = self.get_track_data_from_files(root_files_df_chunk)

        # Write out tracks to csv for first nft file_ids (command line argument).
        if self.file_id < self.num_files_tracks:

            self.write_to_csv(self.file_id, tracks, file_name="tracks")

        print(f"\nProcessing file_id: {self.file_id}")

        # Force a write to the log.
        sys.stdout.flush()

        # Write out events to csv for first nfe file_ids (command line argument).
        if self.file_id < self.num_files_events:

            events = self.get_event_data_from_tracks(tracks)
            self.write_to_csv(self.file_id, events, file_name="events")

        return None

    def get_event_data_from_tracks(self, tracks):

        # Step 0. Clean up the tracks.
        cleaned_tracks = self.clean_up_tracks(tracks)

        # Note that we have to do this twice becuase the event IDs are different after the 
        # dbscan clustering. 
        tracks = self.add_event_info(tracks)

        # Step 1. DBSCAN clustering of events.
        tracks = self.cluster_tracks(tracks)

        # Step 2. Add aggregated event info to tracks.
        tracks = self.add_event_info(tracks)

        # Step 3. Build event df.
        events = self.build_events(tracks)

        return events

    def get_track_data_from_files(self, root_files_df):

        condition = root_files_df["root_file_exists"] == True

        experiment_tracks_list = [
            self.build_tracks_for_single_file(root_files_df_row)
            for index, root_files_df_row in root_files_df[condition].iterrows()
        ]

        tracks_df = pd.concat(experiment_tracks_list, axis=0).reset_index(drop=True)

        tracks_df = self.add_track_info(tracks_df)

        return tracks_df

    def build_tracks_for_single_file(self, root_files_df_row):
        """
        DOCUMENT.
        """

        tracks_df = pd.DataFrame()

        rootfile = uproot.open(root_files_df_row["root_file_path"])

        if "multiTrackEvents;1" in rootfile.keys():

            tracks_root = rootfile["multiTrackEvents;1"]["Event"]["fTracks"]

            for key, value in tracks_root.items():
                # Slice the key so it drops the redundant "fTracks."
                tracks_df[key[9:]] = self.flat(value.array())

        tracks_df["run_id"] = root_files_df_row["run_id"]
        tracks_df["file_id"] = root_files_df_row["file_id"]
        tracks_df["root_file_path"] = root_files_df_row["root_file_path"]
        tracks_df["field"] = root_files_df_row["field"]
        tracks_df["monitor_rate"] = root_files_df_row["monitor_rate"]
        print(tracks_df)
        return tracks_df.reset_index(drop=True)

    def add_track_info(self, tracks):

        # Organize this function a bit.

        tracks["FreqIntc"] = (
            tracks["EndFrequency"] - tracks["EndTimeInRunC"] * tracks["Slope"]
        )
        tracks["TimeIntc"] = (
            tracks["StartTimeInRunC"] - tracks["StartFrequency"] / tracks["Slope"]
        )

        tracks["MeanTrackSNR"] = tracks["TotalTrackSNR"] / tracks["NTrackBins"]

        tracks["set_field"] = tracks["field"].round(decimals=2)

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
        self, tracks, eps=0.005, min_samples=1, features=["EventTimeIntc"]
    ):
        """Notes: 
            * This is really clustering events not track segments. 
            * Default up to 1/24/23 was .003 up to now. 
            * On 1/24/23 1600, Drew is testing how .005 performs. 
        """

        exp_tracks_copy = tracks.copy()
        exp_tracks_copy["event_label"] = 100

        for i, (name, group) in enumerate(
            exp_tracks_copy.groupby(["run_id", "file_id"])
        ):

            print(f"\nClustering: run_id: {name[0]},  file_id: {name[1]}")

            condition = (exp_tracks_copy.run_id == name[0]) & (
                exp_tracks_copy.file_id == name[1]
            )
            print(f"Tracks in file: {condition.sum()}")
            print(f"Fraction of total tracks in dataset: {condition.mean()}")

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

        # Power/SNR metrics.
        tracks["mMeanSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MeanTrackSNR"
        ].transform("mean")
        tracks["sMeanSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MeanTrackSNR"
        ].transform("std")

        tracks["mTotalSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "TotalTrackSNR"
        ].transform("mean")
        tracks["sTotalSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "TotalTrackSNR"
        ].transform("std")

        tracks["mMaxSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MaxTrackSNR"
        ].transform("mean")
        tracks["sMaxSNR"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MaxTrackSNR"
        ].transform("std")

        tracks["mTotalNUP"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "TotalTrackNUP"
        ].transform("mean")
        tracks["sTotalNUP"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "TotalTrackNUP"
        ].transform("std")

        tracks["mTotalPower"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "TotalPower"
        ].transform("mean")
        tracks["sTotalPower"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "TotalPower"
        ].transform("std")

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
            "EventSlope",
            "EventNBins",
            "EventTrackTot",
            "EventFreqIntc",
            "EventTimeIntc",
            "mMeanSNR",
            "sMeanSNR",
            "mTotalSNR",
            "sTotalSNR",
            "mMaxSNR",
            "sMaxSNR",
            "mTotalNUP",
            "sTotalNUP",
            "mTotalPower",
            "sTotalPower",
            "field",
            "set_field",
            "monitor_rate",
        ]
        events = (
            tracks.groupby(["run_id", "file_id", "EventID"])
            .first()
            .reset_index()[event_cols]
        )

        return events

    def add_env_data(self, root_files_df):

        # Step 0: Make sure the root_files_df has a tz aware dt column.
        root_files_df["pst_time"] = root_files_df["root_file_path"].apply(
            lambda x: self.get_utc_time(x)
        )
        root_files_df["pst_time"] = root_files_df["pst_time"].dt.tz_localize(
            "US/Pacific"
        )
        root_files_df["utc_time"] = root_files_df["pst_time"].dt.tz_convert("UTC")

        # Step 1: Add the monitor rate/field data to each file.
        root_files_df = self.add_monitor_rate(root_files_df)
        root_files_df = self.add_field(root_files_df)

        # Step 3. Add the set_field by rounding to nearest 100th place.
        root_files_df["set_field"] = root_files_df["field"].round(decimals=2)

        return root_files_df

    def get_utc_time(self, root_file_path):
        # USED in add_env_data()
        time_str = root_file_path[-28:-9]
        datetime_object = datetime.datetime.strptime(time_str, "%Y-%m-%d-%H-%M-%S")

        return datetime_object

    def get_nearest(self, df, dt):
        # USED in add_env_data()
        # created_at column is the dt column.
        minidx = (dt - df["created_at"]).abs().idxmin()

        return df.loc[[minidx]].iloc[0]

    def add_monitor_rate(self, root_files_df):
        # USED in add_env_data()
        root_files_df["monitor_rate"] = np.nan

        # Step 0. Group by run_id.
        for rid, root_files_df_gb in root_files_df.groupby(["run_id"]):

            # Step 1. Find the extreme times present in the given run_id.
            # The idea is that we want to be careful about the amount of queries we do to get this info.
            # Here we only do one query per run_id (instead of one per file)

            dt_max = root_files_df_gb.utc_time.max().floor("min").tz_localize(None)
            dt_min = root_files_df_gb.utc_time.min().floor("min").tz_localize(None)

            # There is an issue here right now but this will be useful later.
            query = """SELECT m.monitor_id, m.created_at, m.rate
                       FROM he6cres_runs.monitor as m 
                       WHERE m.created_at >= '{}'::timestamp
                           AND m.created_at <= '{}'::timestamp + interval '1 minute'
                    """.format(
                dt_min, dt_max
            )

            monitor_log = he6cres_db_query(query)
            monitor_log["created_at"] = monitor_log["created_at"].dt.tz_localize("UTC")

            for fid, file_path in root_files_df_gb.groupby(["file_id"]):

                if len(file_path) != 1:
                    raise UserWarning(
                        f"There should be only one file with run_id = {rid} and file_id = {fid}."
                    )

                # Get monitor_rate during run.
                monitor_rate = self.get_nearest(
                    monitor_log, file_path.utc_time.iloc[0]
                ).rate

                condition = (root_files_df["run_id"] == rid) & (
                    root_files_df["file_id"] == fid
                )
                root_files_df["monitor_rate"][condition] = monitor_rate

        if root_files_df["monitor_rate"].isnull().values.any():
            raise UserWarning(f"Some monitor_rate data was not collected.")

        return root_files_df

    def add_field(self, root_files_df):

        root_files_df["field"] = np.nan

        # Step 0. Group by run_id.
        for rid, root_files_df_gb in root_files_df.groupby(["run_id"]):

            # Step 1. Find the extreme times present in the given run_id.
            # The idea is that we want to be careful about the amount of queries we do to get this info.
            # Here we only do one query per run_id (instead of one per file)
            dt_max = root_files_df_gb.utc_time.max().floor("min").tz_localize(None)
            dt_min = root_files_df_gb.utc_time.min().floor("min").tz_localize(None)

            # Note that I also need to make sure the field probe was locked!
            query = """SELECT n.nmr_id, n.created_at, n.field
                       FROM he6cres_runs.nmr as n 
                       WHERE n.created_at >= '{}'::timestamp
                           AND n.created_at <= '{}'::timestamp + interval '1 minute'
                           AND n.locked = TRUE
                    """.format(
                dt_min, dt_max
            )

            field_log = he6cres_db_query(query)
            field_log["created_at"] = field_log["created_at"].dt.tz_localize("UTC")

            for fid, file_path in root_files_df_gb.groupby(["file_id"]):

                if len(file_path) != 1:
                    raise UserWarning(
                        f"There should be only one file with run_id = {rid} and file_id = {fid}."
                    )

                # Get field during second of data
                field = self.get_nearest(field_log, file_path.utc_time.iloc[0]).field

                # Now get the nearest rate for each file_id and fill those in!! Then this gets joined with the whole table.
                condition = (root_files_df["run_id"] == rid) & (
                    root_files_df["file_id"] == fid
                )
                root_files_df["field"][condition] = field

        if root_files_df["field"].isnull().values.any():
            raise UserWarning(f"Some rate data was not collected.")

        return root_files_df

    def write_to_csv(self, file_id, df_chunk, file_name):
        print(f"Writing {file_name} data to disk for file_id {file_id}.")
        write_path = self.analysis_dir / Path(f"{file_name}_{file_id}.csv")

        df_chunk.to_csv(write_path)

        return None

    def merge_csvs(self):

        tracks_path_list = [
            self.analysis_dir / Path(f"tracks_{i}.csv")
            for i in range(self.num_files_tracks)
        ]
        events_path_list = [
            self.analysis_dir / Path(f"events_{i}.csv")
            for i in range(self.num_files_events)
        ]

        tracks_path_exists = [path.is_file() for path in tracks_path_list]
        events_path_exists = [path.is_file() for path in events_path_list]

        if not all(tracks_path_exists):
            raise UserWarning(
                f"Not all {self.num_files_tracks} tracks csvs are present for merging csvs."
            )

        if not all(events_path_exists):
            raise UserWarning(
                f"Not all {self.num_files_events} events csvs are present for merging csvs."
            )

        tracks_dfs = [
            pd.read_csv(tracks_path, index_col=0) for tracks_path in tracks_path_list
        ]
        tracks_df = pd.concat(tracks_dfs, ignore_index=True)
        lens = [len(df) for df in tracks_dfs]
        print("\nCombining set of tracks_dfs.\n")
        print("lengths: ", lens)
        print("sum: ", sum(lens))
        print("len single file (sanity check): ", len(tracks_df))
        print("tracks index: ", tracks_df.index)
        print("tracks cols: ", tracks_df.columns)

        events_dfs = [
            pd.read_csv(events_path, index_col=0) for events_path in events_path_list
        ]
        events_df = pd.concat(events_dfs, ignore_index=True)
        lens = [len(df) for df in events_dfs]

        print("\nCombining set of events_dfs.\n")
        print("lengths: ", lens)
        print("sum: ", sum(lens))
        print("len single file (sanity check): ", len(events_df))
        print("events index: ", events_df.index)
        print("events cols: ", events_df.columns)

        tracks_df.to_csv(self.tracks_df_path)
        events_df.to_csv(self.events_df_path)

        for track_path in tracks_path_list:
            track_path.unlink()

        for event_path in events_path_list:
            event_path.unlink()

        return None

    def sanity_check(self):

        desired_path_list = [
            self.root_files_df_path,
            self.tracks_df_path,
            self.events_df_path,
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
                path.unlink()

        # Force a write to the log.
        sys.stdout.flush()

        return None

    def flat(self, jaggedarray) -> np.ndarray:
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


if __name__ == "__main__":
    main()


# def check_if_exists(self, fp):
#     return Path(fp).is_file()

# def set_permissions():

#     set_group = sp.run(["chgrp", "-R", "he6_cres", "katydid_analysis/"])
#     set_permission = sp.run(["chmod", "-R", "774", "katydid_analysis/"])

#     return None


# # Simplify to not have an insert capability.
# # TOD0: Put this in a utility module and have the run_katydid.py use it as well.
# def he6cres_db_query(query: str) -> typing.Union[None, pd.DataFrame]:

#     connection = False
#     try:
#         # Connect to an existing database
#         connection = psycopg2.connect(
#             user="postgres",
#             password="chirality",
#             host="wombat.npl.washington.edu",
#             port="5544",
#             database="he6cres_db",
#         )

#         # Create a cursor to perform database operations
#         cursor = connection.cursor()

#         # Execute a sql_command
#         cursor.execute(query)
#         cols = [desc[0] for desc in cursor.description]
#         query_result = pd.DataFrame(cursor.fetchall(), columns=cols)

#     except (Exception, Error) as error:
#         print("Error while connecting to he6cres_db", error)
#         query_result = None

#     finally:
#         if connection:
#             cursor.close()
#             connection.close()

#     return query_result

# def get_pst_time():
#     tz = pytz.timezone('US/Pacific')
#     pst_now = datetime.datetime.now(tz).replace(microsecond=0).replace(tzinfo=None)
#     return pst_now
