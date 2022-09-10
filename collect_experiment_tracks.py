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

# START HERE.
# LOOK AT 13. WHy no tracks. 
# Then make it work and then work on making this repo pretty and readable. 
# Work on getting sphynx going before you start documenting. 
# Dont' start documenting until it's really there. 


pd.set_option("display.max_columns", 100)


def main():
    """

    TODOS:
    *

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

    # TODO: Build this into a class. Will make it much easier to read..


    analysis_id = args.analysis_id
    run_ids = args.run_ids
    experiment_name = args.experiment_name

    analysis_dir = build_analysis_dir(experiment_name, analysis_id)

    file_df_experiment = get_experiment_files(run_ids, analysis_id)
   
    # TODO: Deal with file_num vs file_id
    #TODO: Build files.csv, tracks.csv. 

    condition = file_df_experiment["root_file_exists"] == True
    # file_df_experiment[condition].apply(lambda row: sanity_check(row), axis = 1)

    print(len(file_df_experiment))
    print(file_df_experiment.columns)
    write_files_df(file_df_experiment, analysis_dir)


    # Go through 50 files at a time.
    n = 50  #chunk row size
    list_file_df = [file_df_experiment[i:i+n] for i in range(0,file_df_experiment.shape[0],n)] 

    for chunk_idx, file_df_chunk in eumerate(list_file_df): 
        print(len(tracks_df_chunk))
        tracks_df_chunk = get_experiment_tracks(file_df_chunk)

        write_tracks_df(chunk_idx, tracks_df_chunk, analysis_dir)



    # # for file_idx in range(len())
    # # TODO: Ok the following is too long (or will be) to write out to a csv all at once. 
    # tracks_df_experiment = get_experiment_tracks(file_df_experiment)



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
        tracks_df_chunk.to_csv(tracks_path, mode='a')

    return None





def build_analysis_dir(experiment_name, analysis_id): 

    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/save_experiments")

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
            raise UserWarning(
                f"run_id {run_id} has no analysis_id {analysis_id}"
            )

    file_df_experiment = pd.concat(file_df_list)

    return file_df_experiment


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
