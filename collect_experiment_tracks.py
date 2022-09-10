#!/usr/bin/env python3
import os
import sys
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
import uproot4
import awkward
import numpy as np

import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc

# Local imports:
# from . import run_katydid as rk
# from rocks_analysis_pipeline.run_katydid import build_file_df_path, check_if_exists

pd.set_option("display.max_columns", 100)


def main():
    """

    TODOS:
    *

    Notes:
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

    analysis_id = args.analysis_id

    # Step 0: Make sure that all of the listed rids/aid exists.
    file_df_list = []
    for run_id in args.run_ids:
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
            raise UserWarning(
                f"run_id {run_id} has no analysis_id {analysis_id}"
            )

    file_df_experiment = pd.concat(file_df_list)

    print(len(file_df_experiment))
    print(file_df_experiment.columns)

    tracks_df_experiment = get_experiment_tracks(file_df_experiment)

    print(len(tracks_df_experiment))
    print(tracks_df_experiment.columns)
    print(tracks_df_experiment.to_string())

    # Now build these two things into a an instance of a data class.

    # Then pickle the object and put it somewhere.

    # Then work on data cleaning and visualization and stuff.


def get_experiment_tracks(file_df_experiment):
    # TODO: Change the run_num to file_id.

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
    """
    Given path to root file containing MultiTrackEvents tree, returns
    a dataframe containing one track per row.

    Args:
        rootfile_path (pathlib.Path): No specifications.

    Returns:
        tracks (pd.DataFrame): No specifications.

    NOTES:
        * We need a place holder to be able to count the total data we're looking at.
    """

    tracks_df = pd.DataFrame()

    rootfile = uproot4.open(root_file_path)

    if "multiTrackEvents" in rootfile.keys():

        tracks_root = rootfile["multiTrackEvents"]["Event"]["fTracks"]

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
