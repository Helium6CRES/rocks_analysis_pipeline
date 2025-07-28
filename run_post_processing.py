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
    arg(
        "-dbscan",
        "--do_dbscan_clustering",
        type=int,
        default=1,
        help="Flag indicating to dbscan cluster colinear events (1) or not (0).",
    )
    arg(
        "-offline_mon",
        "--count_beta_mon_events_offline",
        type=int,
        default=0,
        help="Flag indicating to do an offline beta monitor event count (1) or not (0).",
    )
    arg(
        "-ms_standard",
        "--ms_standard",
        type=int,
        help="""0: Root file names only to second. %Y-%m-%d-%H-%M-%S use for rid 1570 and earlier!
                1: Root file names to ms. "%Y-%m-%d-%H-%M-%S-%f
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

    # Force a write to the log. Should add a time out here? How to do that in python cleanly...
    sys.stdout.flush()

    # Deal with permissions (chmod 774, group he6_cres).
    # Done at the beginning and end of main.
    #set_permissions()

    # Step 0: Build the directory structure out for the experiment results and write the root_file_df to it.

    post_processing = PostProcessing(
        args.run_ids,
        args.analysis_id,
        args.experiment_name,
        args.num_files_tracks,
        args.num_files_events,
        args.file_id,
        args.stage,
        args.do_dbscan_clustering,
        args.count_beta_mon_events_offline,
        args.ms_standard
    )

    # Done at the beginning and end of main.
    #set_permissions()

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
        do_dbscan_clustering,
        count_beta_mon_events_offline,
        ms_standard
    ):

        self.run_ids = run_ids
        self.analysis_id = analysis_id
        self.experiment_name = experiment_name
        self.num_files_tracks = num_files_tracks
        self.num_files_events = num_files_events
        self.file_id = file_id
        self.stage = stage
        self.do_dbscan_clustering = do_dbscan_clustering
        self.count_beta_mon_events_offline = count_beta_mon_events_offline
        self.ms_standard = ms_standard

        self.analysis_dir = self.get_analysis_dir()
        self.root_files_df_path = self.analysis_dir / Path(f"root_files.csv")
        self.tracks_df_path = self.analysis_dir / Path(f"tracks.csv")
        self.events_df_path = self.analysis_dir / Path(f"events.csv")

        # Default field-wise epss for clustering.
        # 6/1/23 (Drew): Note that this is hardcoded so won't work generically for all fields.
        # This is an issue and we should solve it with a spline of these values or something.
        self.set_fields = np.arange(0.75, 3.5, 0.25)
        epss = np.array([0.01, 0.01, 0.007, 0.004, 0.002, 0.001, 0.0008, 0.0005, 0.0003, 0.0002, 0.0001])

        clust_params = {}

        for (set_field, eps) in zip(self.set_fields, epss):

            clust_params.update({set_field: {"eps": eps}})
            clust_params[set_field].update({"features": ["EventPerpInt"]})

        self.clust_params = clust_params

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
                print(
                    f"No processing necessary. Events csv already processed: {events_path}"
                )

            else:

                # Now gather tracks, clean them up, build events. Write csvs to disk.
                self.process_tracks_and_events()

        elif self.stage == 2:

            print("\nPostProcessing stage 2: clean-up.")
            # Start by opening and reading in the file_df.
            self.root_files_df = self.load_root_files_df()

            self.merge_csvs()
            self.sanity_check()

        return None

    def get_analysis_dir(self):

        base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/saved_experiments")

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

        base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/root_files")
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
        slews = self.get_slewtime_data_from_files(root_files_df_chunk)
        tracks = self.get_track_data_from_files(root_files_df_chunk, slews)

        #clean tracks. Add column IsCutPP which is a boolian if it was cut in post processing (here)
        # This trims "barnicles" and bad frequencies
        processed_tracks = self.clean_up_tracks(tracks)

        # Write out tracks to csv for first nft file_ids (command line argument).
        if self.file_id < self.num_files_tracks:

            self.write_to_csv(self.file_id, processed_tracks, file_name="tracks")

        print(f"\nProcessing file_id: {self.file_id}")

        # Force a write to the log.
        sys.stdout.flush()

        # Write out events to csv for first nfe file_ids (command line argument).
        if self.file_id < self.num_files_events:

            events = self.get_event_data_from_tracks(processed_tracks)
            self.write_to_csv(self.file_id, events, file_name="events")

        return None

    def get_event_data_from_tracks(self, tracks):

        # Step 0. Only make events from tracks with cut_condition == False
        cleaned_tracks = tracks[tracks["cut_condition"] == False]

        # Step 1. Add aggregate event data. 
        cleaned_tracks = self.add_event_info(cleaned_tracks)

        # Step 2. Build event df. One row per EventID. 
        events = self.build_events(cleaned_tracks)

        # Step 3. Optional. Cluster events. Use -dbscan flag to change 
        if self.do_dbscan_clustering:
            print("DBSCAN clustering.")
            
            events = self.cluster_and_clean_events(events, diagnostics=True)

        return events

    def get_track_data_from_files(self, root_files_df, slewtimes_df):

        condition = root_files_df["root_file_exists"] == True

        experiment_tracks_list = [
            self.build_tracks_for_single_file(root_files_df_row)
            for index, root_files_df_row in root_files_df[condition].iterrows()
        ]

        tracks_df = pd.concat(experiment_tracks_list, axis=0).reset_index(drop=True)

        tracks_df = self.add_track_info(tracks_df, slewtimes_df)

        return tracks_df

    def get_slewtime_data_from_files(self, root_files_df):

        condition = root_files_df["root_file_exists"] == True

        experiment_slewtimes_list = [
            self.build_slewtimes_for_single_file(root_files_df_row)
            for index, root_files_df_row in root_files_df[condition].iterrows()
        ]

        slewtimes_df = pd.concat(experiment_slewtimes_list, axis=0).reset_index(drop=True)

        return slewtimes_df


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
        tracks_df["arduino_monitor_rate"] = root_files_df_row["arduino_monitor_rate"]

        return tracks_df.reset_index(drop=True)

    def build_slewtimes_for_single_file(self, root_files_df_row):
        """
        check the lines in the csv of root files. Get the path to the SlewTimes.txt and read it as a csv
        """
        slewfile = open(root_files_df_row["slew_file_path"],"r")
        slewtimes_df = pd.read_csv(slewfile, sep=',', header=0)
        #print(slewtimes_df.head(2))
        #print(slewtimes_df.keys())

        #clean up
        slewtimes_df["on_length"] = slewtimes_df["Time_Off"]-slewtimes_df["Time_On"]

        # This is to clean up gaps in the slew times. Bring this back if you need to analyze data from before September 2024. (ie when we upgraded to ExB and moved Vaunix down to bin301)
        #slewtimes_df = slewtimes_df.drop(slewtimes_df[slewtimes_df.on_length < 2e-3].index)
        slewtimes_df["run_id"] = root_files_df_row["run_id"]
        slewtimes_df["file_id"] = root_files_df_row["file_id"]


        return slewtimes_df.reset_index(drop=True)

    def add_track_info(self, tracks, slewtimes):

        # Organize this function a bit.
        tracks["set_field"] = tracks["field"].round(decimals=2)

        tracks["FreqIntc"] = (
            tracks["EndFrequency"] - tracks["EndTimeInRunC"] * tracks["Slope"]
        )
        tracks["TimeIntc"] = (
            tracks["StartTimeInRunC"] - tracks["StartFrequency"] / tracks["Slope"]
        )

        tracks["MeanTrackSNR"] = tracks["TotalTrackSNR"] / tracks["NTrackBins"]

        # Define frequency bins of size 10e6 from 100e6 to 2400e6
        bins = np.arange(100e6, 2400e6 + 10e6, 10e6)
        bin_labels = np.arange(len(bins) - 1)

        # Assign each track to a bin
        tracks['FrequencyBin'] = pd.cut(tracks['StartFrequency'], bins, labels=bin_labels, include_lowest=True)

        # Function to calculate the percentile rank of TotalTrackSNR within each bin
        def calculate_percentile(s):
            return s.rank(pct=True)

        # Apply the function within each bin group and add as a new column
        tracks['MeanTrackSNR_Percentile'] = tracks.groupby(['FrequencyBin','set_field'])['MeanTrackSNR'].transform(calculate_percentile) * 100
        
        # Merge tracks with slewtimes on run_id and file_id
        merged_df = pd.merge(tracks, slewtimes, on=["run_id", "file_id"])
        # Keep rows where StartTimeInRunC is greater than or equal to Time_On
        merged_df = merged_df[merged_df["StartTimeInRunC"] >= merged_df["Time_On"]]
        merged_df_E = merged_df.copy()
        # Rename the StartTimeInRunC column to Event_StartTimeInRunC before grouping
        merged_df_E = merged_df_E.rename(columns={"StartTimeInRunC": "StartTimeInRunC_E"})
        # Group by run_id, file_id, EventID to find the earliest StartTimeInRunC for each EventID
        grouped_df = merged_df_E.groupby(["run_id", "file_id", "EventID"])
        # Find the earliest StartTimeInRunC for each EventID within run_id and file_id
        earliest_start_time = grouped_df["StartTimeInRunC_E"].min().reset_index()
        # Merge back to get the rows with earliest StartTimeInRunC
        merged_earliest = pd.merge(merged_df, earliest_start_time, on=["run_id", "file_id", "EventID"])
        #this should keep only rows where the event the track is in has a later startTimeInRunC than the Time_On
        merged_earliest = merged_earliest[merged_earliest["StartTimeInRunC_E"] >= merged_earliest["Time_On"]]
        # Group by run_id, file_id, TrackID and calculate the cumulative count to create Acq_ID
        merged_earliest['Acq_ID'] = merged_earliest.groupby(['run_id', 'file_id', 'TrackID']).cumcount() + 1
        # Find the indices of the rows with the highest Acq_ID within each group
        max_acq_id_indices = merged_earliest.groupby(['run_id', 'file_id', 'TrackID'])['Acq_ID'].idxmax()

        # Filter the DataFrame to keep only the rows with the highest Acq_ID
        tracks = merged_earliest.loc[max_acq_id_indices]

        # Drop the StartTimeInRunC_E column
        tracks = tracks.drop(columns=['StartTimeInRunC_E'])

        # Calculate StartTimeInAcq and EndTimeInAcq
        tracks["StartTimeInAcq"] = tracks["StartTimeInRunC"] - tracks["Time_On"]
        tracks["EndTimeInAcq"] = tracks["EndTimeInRunC"] - tracks["Time_On"]

        tracks["FreqIntA"] = (
            tracks["EndFrequency"] - tracks["EndTimeInAcq"] * tracks["Slope"]
        )
        tracks["TimeIntA"] = (
            tracks["StartTimeInAcq"] - tracks["StartFrequency"] / tracks["Slope"]
        )

        intc_info = (
            tracks.groupby(["run_id", "file_id", "EventID"])
            .agg(
                TimeIntc_mean=("TimeIntc", "mean"),
                TimeIntc_std=("TimeIntc", "std"),
                TimeIntA_mean=("TimeIntA", "mean"),
                TimeIntA_std=("TimeIntA", "std"),
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

        # Add the cut condition as a new column
        tracks["cut_condition"] = cut_condition
        print("number of tracks cut: ", np.sum(cut_condition))
        return tracks

    def create_track_cleaning_cut(self, tracks, cols, cut_levels):

        conditions = [
            (
                (np.abs((tracks[col] - tracks[col + "_mean"]) / tracks[col + "_std"]))
                < cut_level
            )
            for col, cut_level in zip(cols, cut_levels)
        ]
        # Add the new condition for StartFrequency to cut vaunix image (TEMP!)
        start_freq_cond = (
            ((tracks["StartFrequency"] >= (1.1e9 - 1.5e6)) & (tracks["StartFrequency"] <= (1.1e9 + 1.0e6)))
            | ((tracks["StartFrequency"] >= (1.3e9 - 1.5e6)) & (tracks["StartFrequency"] <= (1.3e9 + 1.0e6)))
        )
        conditions.append(start_freq_cond)

        # Combine all conditions
        condition_tot = np.ones_like(conditions[0])
        for condition in conditions:
            condition_tot = condition_tot & condition

        return start_freq_cond

    def cluster_and_clean_events(self, events, diagnostics=False):

        if diagnostics:

            # Take stock of what events were like before the clustering.
            pre_clust_counts = events.groupby("set_field").file_id.count()
            pre_clust_summary_mean = events.groupby("set_field").mean()
            pre_clust_summary_std = events.groupby("set_field").std()

        # cluster
        events = self.cluster_events(events)

        # cleanup
        events = self.update_event_info(events)

        # Ensures one row per unique EventID after clustering.
        events = self.build_events(events)

        if diagnostics:

            # Take stock of what events were like after the clustering.
            post_clust_counts = events.groupby("set_field").file_id.count()
            post_clust_summary_mean = events.groupby("set_field").mean()
            post_clust_summary_std = events.groupby("set_field").std()

            print("Summary of clustring: \n")
            print(
                f"\nFractional reduction in counts from clustering:",
                post_clust_counts / pre_clust_counts,
            )
            print("\nPre-clustering means:")
            print(pre_clust_summary_mean)
            print("\nPre-clustering stds:")
            print(pre_clust_summary_std)
            print("\nPost-clustering means:")
            print(post_clust_summary_mean)
            print("\nPost-clustering stds:")
            print(post_clust_summary_std)

        return events

    def cluster_events(self, events):
        """ 
        """

        events_copy = events.copy()
        events_copy["event_label"] = np.NaN

        #DBSCAN is now only on events with the same run_id, file_id, Acq_ID. 
        #Note that EventID is now unique to an acquisition, not a second
        for i, (name, group) in enumerate(events_copy.groupby(["run_id", "file_id", "Acq_ID"])):

            #This is to try to be robust against set_field being wrong from user error when taking data.
            #use field from NMR instead and round to hope you get one of the fields in the clustering params
            set_field = group.field.mean().round(2)

            condition = (events_copy.run_id == name[0]) & (events_copy.file_id == name[1]) & (events_copy.Acq_ID == name[2])

            events_copy.loc[condition, "event_label"] = self.dbscan_clustering(
                events_copy[condition],
                features=self.clust_params[set_field]["features"],
                eps=self.clust_params[set_field]["eps"],
                min_samples=1,
            )
        events_copy["EventID"] = events_copy["event_label"] + 1

        return events_copy

    def dbscan_clustering(self, df, features: list, eps: float, min_samples: int):

        # Previously (incorrectly) used the standardscaler but
        # This meant there was a different normalization on each file!
        # X_norm = StandardScaler().fit_transform(df[features])

        # Compute DBSCAN
        db = DBSCAN(eps=eps, min_samples=min_samples).fit(df[features])
        labels = db.labels_

        return labels

    def update_event_info(self, events_in: pd.DataFrame) -> pd.DataFrame:

        events = events_in.copy()
        events = events.loc[:, ~events.columns.duplicated()]

        events["EventStartTime"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventStartTime"
        ].transform("min")

        events["EventStartTimeInAcq"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventStartTimeInAcq"
        ].transform("min")

        events["EventEndTime"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventEndTime"
        ].transform("max")

        events["EventEndTimeInAcq"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventEndTimeInAcq"
        ].transform("max")

        events["EventStartFreq"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventStartFreq"
        ].transform("min")
        events["EventEndFreq"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventEndFreq"
        ].transform("max")

        events["EventTimeLength"] = events["EventEndTime"] - events["EventStartTime"]
        events["EventFreqLength"] = events["EventEndFreq"] - events["EventStartFreq"]
        events["EventNBins"] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
            "EventNBins"
        ].transform("sum")

        events["EventSlope"] = events["EventFreqLength"] / events["EventTimeLength"]

        #Event Acq_ID is an average of component track Acq_ID, 
        #as these are assigned on the basis of that track's event, non-int Acq_ID indicats a bug
        cols_to_average_over = [
            "EventTrackCoverage",
            "EventTrackTot",
            "EventFreqIntc",
            "EventTimeIntc",
            "EventFreqIntA",
            "EventTimeIntA",
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
            "mMeanSNR_Percentile",
            "sMeanSNR_Percentile",
            "field",
            "set_field",
            "arduino_monitor_rate",
            "FieldAveSlope",
            "EventPerpInt",
        ]
        for col in cols_to_average_over:

            events[col] = events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])[
                col
            ].transform("mean")

        return events

    def build_events(self, events: pd.DataFrame) -> pd.DataFrame:

        event_cols = [
            "run_id",
            "file_id",
            "EventID",
            "Acq_ID",
            "EventStartTime",
            "EventStartTimeInAcq",
            "EventEndTime",
            "EventEndTimeInAcq",
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
            "EventFreqIntA",
            "EventTimeIntA",
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
            "mMeanSNR_Percentile",
            "sMeanSNR_Percentile",
            "field",
            "set_field",
            "arduino_monitor_rate",
            "FieldAveSlope",
            "EventPerpInt",
        ]

        events = (
            events.groupby(["run_id", "file_id", "Acq_ID", "EventID"])
            .first()
            .reset_index()[event_cols]
        )

        return events


    def add_event_info(self, tracks_in: pd.DataFrame) -> pd.DataFrame:

        tracks = tracks_in.copy()

        tracks["Acq_ID"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "Acq_ID"
        ].transform("mean")

        tracks["EventStartTime"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "StartTimeInRunC"
        ].transform("min")
        
        tracks["EventStartTimeInAcq"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "StartTimeInAcq"
        ].transform("min")

        tracks["EventEndTime"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "EndTimeInRunC"
        ].transform("max")

        tracks["EventEndTimeInAcq"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "EndTimeInAcq"
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

        tracks["mMeanSNR_Percentile"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MeanTrackSNR_Percentile"
        ].transform("mean")
        tracks["sMeanSNR_Percentile"] = tracks.groupby(["run_id", "file_id", "EventID"])[
            "MeanTrackSNR_Percentile"
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

        tracks["EventFreqIntA"] = (
            tracks["EventEndFreq"] - tracks["EventEndTimeInAcq"] * tracks["EventSlope"]
        )
        tracks["EventTimeIntA"] = (
            tracks["EventStartTimeInAcq"] - tracks["EventStartFreq"] / tracks["EventSlope"]
        )

        approx_slopes = self.set_fields.copy()
        for i, field in enumerate(self.set_fields):
            approx_slopes[i] = self.get_slope(field)*1e-9
        print("approx_slopes: ",approx_slopes)

        tracks['FieldAveSlope'] = approx_slopes[np.searchsorted(self.set_fields, tracks['set_field'])]
        tracks['Eventb'] = 0.6+1/tracks['FieldAveSlope']*0.5
        tracks['Eventtheta'] = np.arctan(1/tracks['FieldAveSlope'])
        tracks['Eventx0'] = (tracks['Eventb']-tracks['EventFreqIntc']*1e-9)/(tracks['EventSlope']*1e-9+(1/tracks['FieldAveSlope']))

        #Make new column for the perp intercept.
        tracks['EventPerpInt'] = (tracks['Eventb']-tracks['EventFreqIntc']*1e-9)/((tracks['EventSlope']*1e-9+(1/tracks['FieldAveSlope'])) * np.cos(tracks['Eventtheta']))
        

        return tracks


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
        root_files_df = self.add_arduino_monitor_rate(root_files_df)
        root_files_df = self.add_field(root_files_df)
        if self.count_beta_mon_events_offline:
            root_files_df = self.add_offline_monitor_counts(root_files_df)
        root_files_df = self.add_pressures(root_files_df)
        root_files_df = self.add_temps(root_files_df)

        # Step 3. Add the set_field by rounding to nearest 100th place.
        root_files_df["set_field"] = root_files_df["field"].round(decimals=2)

        return root_files_df

    def get_utc_time(self, root_file_path):
        # USED in add_env_data()
        if self.ms_standard:
            #print("User specified run_ids are all in ms standard.")
            time_str = root_file_path[-32:-9]
            time_str_padded = time_str + "000"  # Pad with zeros to get microseconds
            datetime_object = datetime.datetime.strptime(time_str, "%Y-%m-%d-%H-%M-%S-%f")

        else:
            print("User specified run_ids are all in second standard.")
            time_str = root_file_path[-28:-9]
            datetime_object = datetime.datetime.strptime(time_str, "%Y-%m-%d-%H-%M-%S")

        return datetime_object

    def get_nearest(self, df, dt):
        # USED in add_env_data()
        # created_at column is the dt column.
        minidx = (dt - df["created_at"]).abs().idxmin()

        return df.loc[[minidx]].iloc[0]

    def add_arduino_monitor_rate(self, root_files_df):
        # USED in add_env_data()
        root_files_df["arduino_monitor_rate"] = np.nan

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

                # Get arduino_monitor_rate during run.
                arduino_monitor_rate = self.get_nearest(
                    monitor_log, file_path.utc_time.iloc[0]
                ).rate

                condition = (root_files_df["run_id"] == rid) & (
                    root_files_df["file_id"] == fid
                )
                root_files_df["arduino_monitor_rate"][condition] = arduino_monitor_rate

        if root_files_df["arduino_monitor_rate"].isnull().values.any():
            raise UserWarning(f"Some arduino_monitor_rate data was not collected.")

        return root_files_df

    def add_offline_monitor_counts(self, root_files_df):
        # For now works with just the trigger channel (CH4)
        # Does not computer offline coincidence!
        # USED in add_env_data()
        root_files_df["offline_monitor_counts"] = np.nan

        # Step 0. Group by run_id.
        for rid, root_files_df_gb in root_files_df.groupby(["run_id"]):

            # Step 1. Find the ealiest UTC time present in the given run_id.
            dt_min = root_files_df_gb.utc_time.min().floor("min").tz_localize(None)

            # Get runname in caen_runs table nearest before the earliest time in run_id. logged_at also UTC
            query = """SELECT cr.caen_run_id, cr.runname, cr.logged_at
                       FROM he6cres_runs.caen_runs as cr 
                       WHERE cr.logged_at <= '{}'::timestamp
                       ORDER BY cr.caen_run_id DESC LIMIT 1
                    """.format(dt_min)

            caen_log = he6cres_db_query(query)
            if caen_log.empty:
                raise UserWarning("No matching caen_run found in the database.")
            else:
                caen_run_path = Path(caen_log['runname'].iloc[0])

                # Build path to run.info on rocks
                rocks_caen_run_info_path = Path('/data/raid2/eliza4/he6_cres/betamon/caen') / caen_run_path.name / Path('run.info')

                # Read in run.info and extract time.start
                time_start = None
                if rocks_caen_run_info_path.exists():
                    with rocks_caen_run_info_path.open('r') as f:
                        for line in f:
                            if line.startswith('time.start='):
                                time_start = line.split('=')[1].strip()
                                break
                # Define the format of the input string
                time_format = "%Y/%m/%d %H:%M:%S.%f%z"

                # Convert to a datetime object. includes the parsed time and timezone offset for PST
                dt = datetime.datetime.strptime(time_start, time_format)
                # Convert to UTC
                dt_utc = dt.astimezone(datetime.timezone.utc)
                # Convert to numpy.datetime64
                caen_run_time_start = np.datetime64(dt_utc)

                # Build path to compass data csv on rocks
                rocks_caen_run_data_path = Path('/data/raid2/eliza4/he6_cres/betamon/caen') / caen_run_path.name / Path(f'RAW/DataR_CH4@DT5725_1146_{caen_run_path.name}.csv')
                # Read in the compass data csv to caen_df
                caen_df = pd.read_csv(rocks_caen_run_data_path, index_col=0, sep=';')

                # Add new column to caen_df for absolute UTC timestamp for each hit
                # Convert TIMETAG from picoseconds to nanoseconds
                caen_df['TIMETAG_ns'] = caen_df['TIMETAG'] / 1_000
                # Use 'ns' as the unit
                caen_df['TIMETAG_abs'] = caen_run_time_start + pd.to_timedelta(caen_df['TIMETAG_ns'], unit='ns')
                caen_df['TIMETAG_abs'] = caen_df['TIMETAG_abs'].dt.tz_localize("UTC")
                # print(caen_df)

                condition = (root_files_df["run_id"] == rid)
                # Apply the monitor event counting function to each row (ie each 1s CRES file) in this run_id
                root_files_df.loc[condition, 'offline_monitor_counts'] = root_files_df_gb.apply(self.count_events, caen_df=caen_df, axis=1)

        return root_files_df

    # Define a function to count events for each row in df_A
    def count_events(self, row, caen_df):
        end_time = row['utc_time'] #spec(k) file name is immediatly after write time, not before
        start_time = end_time - pd.Timedelta(seconds=1)
        #print("start time: ", start_time, " end time: ", end_time)
        #Works fine to compare datetime and np.datetime64 objects!
        #print(caen_df[(caen_df['TIMETAG_abs'] > start_time) & (caen_df['TIMETAG_abs'] < end_time)])
        return caen_df[(caen_df['TIMETAG_abs'] > start_time) & (caen_df['TIMETAG_abs'] < end_time)].shape[0]

    def count_events_efficient(self, row, caen_df):
        # Ensure TIMETAG_abs is a NumPy datetime64 array
        timetag_values = caen_df['TIMETAG_abs'].to_numpy()

        # Convert start_time and end_time to NumPy datetime64
        start_time = np.datetime64(row['utc_time'])
        end_time = start_time + np.timedelta64(1, 's')  # Add 1 second
        print("start time: ", start_time, " end time: ", end_time)
        # Use searchsorted to find the indices
        start_idx = np.searchsorted(timetag_values, start_time, side='left')
        end_idx = np.searchsorted(timetag_values, end_time, side='right')
        print(timetag_values[start_idx])
        print(timetag_values[end_idx])
        # Return the count of events within the range
        return end_idx - start_idx        

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
            if field_log.empty:
                field_log["created_at"] = np.nan
            else:    
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
                #raise UserWarning(f"Some rate data was not collected.")
                print("Some nmr data was not collected.")

        return root_files_df

    def add_pressures(self, root_files_df):
        # Define the list of gases
        gases = [
            "nitrogen", "helium", "co2", "hydrogen", 
            "water", "oxygen", "krypton", "argon", 
            "cf3", "a19", "total"
        ]

        # Initialize the columns for each gas with NaN
        root_files_df = root_files_df.assign(**{gas: np.nan for gas in gases})


        # Step 0. Group by run_id.
        for rid, root_files_df_gb in root_files_df.groupby(["run_id"]):

            # Step 1. Find the extreme times present in the given run_id.
            # The idea is that we want to be careful about the amount of queries we do to get this info.
            # Here we only do one query per run_id (instead of one per file)
            dt_max = root_files_df_gb.utc_time.max().floor("min").tz_localize(None)
            dt_min = root_files_df_gb.utc_time.min().floor("min").tz_localize(None)

            # Note that I also need to make sure the field probe was locked!
            query = """SELECT r.utc_write_time, r.nitrogen, r.helium, r.co2, r.hydrogen, r.water, r.oxygen, r.krypton, r.argon, r.cf3, r.a19, r.total
                       FROM he6cres_runs.rga as r 
                       WHERE r.utc_write_time >= '{}'::timestamp
                           AND r.utc_write_time <= '{}'::timestamp + interval '1 minute'
                           AND r.time_since_write < 60.0
                    """.format(
                dt_min, dt_max
            )

            rga_log = he6cres_db_query(query)
            if rga_log.empty:
                rga_log["created_at"] = np.nan
            else:    
                # This is NOT the same as the created_at field in the db, I'm re-naming the more accurate utc_write_time
                # to created_at for consistancy in get_nearest()
                rga_log["created_at"] = rga_log["utc_write_time"].dt.tz_localize("UTC")

                for fid, file_path in root_files_df_gb.groupby(["file_id"]):

                    if len(file_path) != 1:
                        raise UserWarning(
                            f"There should be only one file with run_id = {rid} and file_id = {fid}."
                        )

                    nearest_row = self.get_nearest(rga_log, file_path.utc_time.iloc[0])
                    # Assign values for all gases at once
                    condition = (root_files_df["run_id"] == rid) & (root_files_df["file_id"] == fid)
                    root_files_df.loc[condition, gases] = nearest_row[gases].values


            if root_files_df["total"].isnull().values.any():
                print("Some rga data was not collected.")

        return root_files_df

    def add_temps(self, root_files_df):
        # Define the list of endpoints
        epts = [7,8,9,10,11,12,13,14]
        sensor_names = ['A','B','C','D','E','F','G','H']

        # Initialize the columns for each endpoint with NaN
        root_files_df = root_files_df.assign(**{sensor: np.nan for sensor in sensor_names})

        # Step 0. Group by run_id.
        for rid, root_files_df_gb in root_files_df.groupby(["run_id"]):

            # Step 1. Find the extreme times present in the given run_id.
            # The idea is that we want to be careful about the amount of queries we do to get this info.
            # Here we only do one query per run_id (instead of one per file)
            dt_max = root_files_df_gb.utc_time.max().floor("min").tz_localize(None)
            dt_min = root_files_df_gb.utc_time.min().floor("min").tz_localize(None)

            # 
            query = """SELECT e.endpoint_id, e.timestamp, e.value_raw
                       FROM public.endpoint_numeric_data as e
                       WHERE e.timestamp >= '{}'::timestamp
                           AND e.timestamp <= '{}'::timestamp + interval '1 minute'
                    """.format(
                dt_min, dt_max
            )

            rga_log = he6cres_db_query(query)

            if not rga_log.empty:
                rga_log["created_at"] = rga_log["timestamp"].dt.tz_localize("UTC")

                # Filter and group `rga_log` by `endpoint_id` once
                rga_log_grouped = {ept: grp for ept, grp in rga_log.groupby("endpoint_id")}

                for fid, file_path in root_files_df_gb.groupby("file_id"):
                    if len(file_path) != 1:
                        raise UserWarning(f"There should be only one file with run_id = {rid} and file_id = {fid}.")

                    file_time = file_path.utc_time.iloc[0]

                    # Iterate over the endpoints and assign temperatures
                    for i, ept in enumerate(epts):
                        rga_log_ept = rga_log_grouped.get(ept)
                        if rga_log_ept is not None:
                            temp_value = self.get_nearest(rga_log_ept, file_time)["value_raw"]

                            # Apply the value to the relevant rows
                            condition = (root_files_df["run_id"] == rid) & (root_files_df["file_id"] == fid)
                            root_files_df.loc[condition, sensor_names[i]] = temp_value

        # Check for missing data
        if root_files_df[sensor_names].isnull().any().any():
            print("Some temp data was not collected.")

        return root_files_df

    def write_to_csv(self, file_id, df_chunk, file_name):
        print(f"Writing {file_name} data to disk for file_id {file_id}.")
        write_path = self.analysis_dir / Path(f"{file_name}_{file_id}.csv")

        df_chunk.to_csv(write_path)

        return None

    def merge_csvs(self):

        # Drew, 6/6/23: Editing this to address Heather's issue.   
        max_fid = self.root_files_df.file_id.max()
        if self.num_files_events > max_fid: 
            # The +1 is necessary because max_fid is zero indexed.
            self.num_files_events = max_fid + 1

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
            print(f"Not all {self.num_files_tracks} tracks csvs are present for merging csvs.")

        if not all(events_path_exists):
            print(f"Not all {self.num_files_events} events csvs are present for merging csvs.")

        # Filter the lists to include only the paths that exist
        tracks_path_list = [path for path in tracks_path_list if path.is_file()]
        events_path_list = [path for path in events_path_list if path.is_file()]

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

    def get_slope(self, true_field, frequency: float = 19.15e9):

        approx_power = sc.power_larmor(true_field, frequency)
        approx_energy = sc.freq_to_energy(frequency, true_field)
        approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

        return approx_slope


if __name__ == "__main__":
    main()
