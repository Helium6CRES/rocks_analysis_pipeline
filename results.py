#!/usr/bin/env python3
from itertools import compress
from pathlib import Path
import shutil
import subprocess
import pathlib
import paramiko
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import typing
import uproot4


class ExperimentResults:
    def __init__(
        self,
        local_dir,
        experiment_name,
        analysis_id,
        include_root_files=True,
        max_root_files_to_grab=10,
        rebuild_experiment_dir=False,
        rocks_username="drewbyron",
        rocks_IP="172.25.100.1",
    ):

        # Attributes.
        self.local_dir = Path(local_dir)
        self.experiment_name = experiment_name
        self.analysis_id = analysis_id
        self.include_root_files = include_root_files
        self.max_root_files_to_grab = max_root_files_to_grab
        self.rebuild_experiment_dir = rebuild_experiment_dir
        self.rocks_username = rocks_username
        self.rocks_IP = rocks_IP

        self.rocks_base_path = Path(
            "/data/eliza4/he6_cres/katydid_analysis/saved_experiments"
        )
        self.experiment_dir_name = Path(f"{experiment_name}_aid_{analysis_id}")
        self.experiment_dir_rocks = self.rocks_base_path / self.experiment_dir_name
        self.experiment_dir_loc = self.local_dir / self.experiment_dir_name

        # Grab the plot_settings or else set to default:
        self.plt_settings = {
            "figsize": (12, 6),
            "colors": ["b", "r", "g", "c", "m", "k"],
        }

        # Step 0. Build the local experiment dir.
        self.build_local_experiment_dir()

        # Step 1. Build csvs into attributes.
        self.build_result_attributes()

        # Step 2. Grab root files (if desired).
        self.grab_root_files()

    def grab_root_files(self):

        if self.include_root_files:

            self.root_file_dir = self.experiment_dir_loc / Path("root_files")

            self.root_files_loc = (
                self.tracks.groupby(["root_file_path"])
                .first()[["run_id", "file_id"]]
                .reset_index()
            )
            self.root_files_loc["root_file_path"] = self.root_files_loc[
                "root_file_path"
            ].apply(lambda row: self.make_path(row))
            self.root_files_loc["root_file_path_loc"] = self.root_files_loc[
                "root_file_path"
            ].apply(lambda row: self.make_local_path(row))

            if not self.root_file_dir.is_dir():
                print(f"Making {self.root_file_dir}")
                self.root_file_dir.mkdir()

            remote_paths = self.root_files_loc["root_file_path"].to_list()
            local_paths = self.root_files_loc["root_file_path_loc"].to_list()

            mask = [not local_path.exists() for local_path in local_paths]

            remote_paths_missing = list(compress(remote_paths, mask))
            local_paths_missing = list(compress(local_paths, mask))

            if len(remote_paths_missing) != 0:

                self.copy_from_rocks(
                    remote_paths_missing[: self.max_root_files_to_grab],
                    local_paths_missing[: self.max_root_files_to_grab],
                )
            else:
                print(
                    f"All {len(remote_paths)} root files are already present here: {self.root_file_dir}"
                )

        return None

    def make_path(self, x):
        return Path(x)

    def make_local_path(self, x):
        return self.root_file_dir / x.name

    def visualize(
        self,
        run_id,
        file_id,
        config={
            "tracks": {"show": True, "EventIDs": [], "alpha": 0.5},
            "events": {"show": True, "alpha": 1.0},
            "sparse_spec": {"show": True, "frac_pts": 1.0, "mrk_sz": 0.1, "alpha": 0.1},
        },
        plt_settings=None,
    ):

        if plt_settings is not None:
            self.plt_settings.update((k, plt_settings[k]) for k in plt_settings.keys())

        plt.close("all")
        fig, ax = plt.subplots(figsize=self.plt_settings["figsize"])

        if config["events"]["show"]:
            self.viz_events(ax, run_id, file_id, config)
        if config["tracks"]["show"]:
            self.viz_tracks(ax, run_id, file_id, config)
        if config["sparse_spec"]["show"]:
            self.viz_sparse_spec(ax, run_id, file_id, config)

        # Finish the plot.
        ax.set_ylabel("MHz")
        ax.set_xlabel("Time (s)")
        ax.set_title(f"run_id: {run_id}, file_id: {file_id}")
        plt.show()

        return None

    def viz_events(self, ax, run_id, file_id, config):

        condition = (self.events.run_id == run_id) & (self.events.file_id == file_id)
        if condition.sum() == 0:
            print(f"Warning: no event data for run_id {run_id}, file_id {file_id}")

        for EventID, event in self.events[condition].iterrows():

            time_coor = np.array(
                [float(event["EventStartTime"]), float(event["EventEndTime"])]
            )
            freq_coor = np.array(
                [float(event["EventStartFreq"]), float(event["EventEndFreq"])]
            )

            ax.plot(
                time_coor,
                freq_coor * 1e-6,
                "o-",
                color=self.plt_settings["colors"][
                    int(event.EventID) % len(self.plt_settings["colors"])
                ],
                markersize=0.5,
                alpha=config["events"]["alpha"],
                label="EventID = {}".format(event.EventID),
            )
        ax.legend(loc="upper left")

        return None

    def viz_tracks(self, ax, run_id, file_id, config):

        condition = (self.tracks.run_id == run_id) & (self.tracks.file_id == file_id)

        if condition.sum() == 0:
            print(f"Warning: no track data for run_id {run_id}, file_id {file_id}")
        else:
            for index, row in self.tracks[condition].iterrows():

                time_coor = np.array([row["StartTimeInRunC"], row["EndTimeInRunC"]])
                freq_coor = np.array([row["StartFrequency"], row["EndFrequency"]])

                ax.plot(
                    time_coor,
                    freq_coor * 1e-6,
                    "yo-",
                    markersize=0.5,
                    alpha=config["events"]["alpha"],
                )

            for i, EventID in enumerate(config["tracks"]["EventIDs"]):

                first_track_in_event = True

                for index, row in self.tracks[condition][
                    self.tracks[condition]["EventID"] == EventID
                ].iterrows():

                    time_coor = np.array([row["StartTimeInRunC"], row["EndTimeInRunC"]])
                    freq_coor = np.array([row["StartFrequency"], row["EndFrequency"]])

                    if first_track_in_event:
                        ax.plot(
                            time_coor,
                            freq_coor * 1e-6,
                            "o-",
                            color=self.plt_settings["colors"][
                                i % len(self.plt_settings["colors"])
                            ],
                            markersize=0.5,
                            alpha=config["events"]["alpha"],
                            label="EventID = {} (tracks)".format(EventID),
                        )

                        first_track_in_event = False
                    else:
                        ax.plot(
                            time_coor,
                            freq_coor * 1e-6,
                            "o-",
                            color=self.plt_settings["colors"][
                                i % len(self.plt_settings["colors"])
                            ],
                            markersize=0.5,
                            alpha=config["events"]["alpha"],
                        )

                ax.legend(loc="upper left")
        return None

    def viz_sparse_spec(self, ax, run_id, file_id, config):
        """
        DOCUMENT.
        """
        # Root file path.
        condition = (self.root_files_loc.run_id == run_id) & (
            self.root_files_loc.file_id == file_id
        )
        if condition.sum() == 0:
            print(f"Warning: no root file for run_id {run_id}, file_id {file_id}")
        else:
            path = self.root_files_loc[condition].root_file_path_loc.iloc[0]
            # Open rootfile.
            rootfile = uproot4.open(path)

            if config["sparse_spec"]["frac_pts"] != 0.0:

                slice_by = int(1 / config["sparse_spec"]["frac_pts"])
                Time = rootfile["discPoints1D"]["TimeInRunC"].array()[::slice_by]
                Abscissa = rootfile["discPoints1D"]["Abscissa"].array()[::slice_by]
                ax.plot(
                    Time,
                    Abscissa * 1e-6,
                    "bo",
                    markersize=config["sparse_spec"]["mrk_sz"],
                    alpha=config["sparse_spec"]["alpha"],
                )

        return None

    def build_result_attributes(self):

        self.root_files_path = self.experiment_dir_loc / Path("root_files.csv")
        self.tracks_path = self.experiment_dir_loc / Path("tracks.csv")
        self.events_path = self.experiment_dir_loc / Path("events.csv")

        print("\nCollecting root_files, tracks, and events.")
        self.root_files = pd.read_csv(self.root_files_path, index_col=0)
        self.tracks = pd.read_csv(self.tracks_path, index_col=0)
        self.events = pd.read_csv(self.events_path, index_col=0)

        # TODO: Are empty files being missed if we use the below?
        self.run_ids = sorted(self.events["run_id"].unique().tolist())
        self.file_ids = sorted(self.events["file_id"].unique().tolist())

        return None

    def build_local_experiment_dir(self):

        if self.experiment_dir_loc.exists():

            if self.rebuild_experiment_dir:
                print("Rebuilding local experiment dir")
                shutil.rmtree(str(self.experiment_dir_loc))
                self.copy_remote_experiment_dir()

            else:
                print("Keeping existing experiment directory.")
        else:

            self.copy_remote_experiment_dir()

        return None

    def copy_remote_experiment_dir(self):
        print(
            f"Copying analysis directory from rocks. This may take a few minutes depending on the size of {str(self.experiment_dir_rocks)} the speed of the connection"
        )
        scp_run_list = [
            "scp",
            "-r",
            f"{self.rocks_username}@{self.rocks_IP}:{str(self.experiment_dir_rocks)}",
            str(self.local_dir),
        ]
        self.execute(scp_run_list)

        return None

    def execute(self, cmd):

        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for line in popen.stdout:
            print(line, end="")
        popen.stdout.close()
        return_code = popen.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)
        return None

    def copy_from_rocks(self, remote_paths, local_paths):
        try:

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            ssh.connect(hostname=self.rocks_IP, username=self.rocks_username)
            sftp = ssh.open_sftp()

            for remote_path, local_path in zip(remote_paths, local_paths):

                if local_path.is_file():
                    print("File already exists locally: {}".format(remote_path.name))

                print("Copying from remote path: ", str(remote_path))
                sftp.get(str(remote_path), str(local_path), prefetch=True)
                print("Got above file. Put it: {} ".format(local_path))

        finally:
            if sftp:
                print("Closing sftp connection to rocks.")
                sftp.close()

            if ssh:
                print("Closing ssh connection to rocks.")
                ssh.close()

        return None
