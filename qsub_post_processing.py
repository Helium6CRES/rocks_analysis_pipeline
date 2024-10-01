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
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

# Local imports.
sys.path.append("/data/eliza4/he6_cres/simulation/he6-cres-spec-sims")
import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc


# Local imports.
from rocks_utility import set_permissions


# Import options.
pd.set_option("display.max_columns", 100)


def main():
    """
    DOCUMENT
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
    arg("-dbscan",
        "--do_dbscan_clustering",
        type=int,
        default=1, 
        help="Flag indicating to dbscan cluster colinear events (1) or not (0)."
    )
    arg(
        "-ms_standard",
        "--ms_standard",
        type=int,
        help="""0: Root file names only to second. %Y-%m-%d-%H-%M-%S
                1: Root file names to ms. "%Y-%m-%d-%H-%M-%S-%f"
            """,
    )

    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")

    args = par.parse_args()

    tlim = "12:00:00" if args.tlim is None else args.tlim[0]

    # Force a write to the log. Add a timeout here in the future?
    sys.stdout.flush()

    # Deal with permissions (chmod 770, group he6_cres).
    # Done at the beginning and end of qsub main.
    #set_permissions()

    # ./rocks_analysis_pipeline/post_processing.py -rids 440 439 377 376 375 374 373 -aid 16 -name "demo1" -nft 1 -nfe 1
    # Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.
    # DREW_02082023: Adding in the singularity command first as this should help with python package versioning issues.
    con = "\"singularity exec --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif /bin/bash -c $'source /data/eliza4/he6_cres/.bashrc {} ".format(
        r"\n"
    )
    base_post_processing_cmd = 'python3 /data/eliza4/he6_cres/rocks_analysis_pipeline/run_post_processing.py -rids {} -aid {} -name "{}" -nft {} -nfe {} -fid {} -stage {} -dbscan {} -ms_standard {}'
    rids_formatted = " ".join((str(rid) for rid in args.run_ids))
    if args.stage == 0:
        file_id = -1
        post_processing_cmd = base_post_processing_cmd.format(
            rids_formatted,
            args.analysis_id,
            args.experiment_name,
            args.num_files_tracks,
            args.num_files_events,
            file_id,
            args.stage,
            args.do_dbscan_clustering,
            args.ms_standard
        )
        cmd = con + f"{post_processing_cmd}'\""
        print(cmd)

        qsub_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)

    if args.stage == 1:

        files_to_process = args.num_files_events
        print(f"Submitting {files_to_process} jobs.")

        for file_id in range(files_to_process):

            post_processing_cmd = base_post_processing_cmd.format(
                rids_formatted,
                args.analysis_id,
                args.experiment_name,
                args.num_files_tracks,
                args.num_files_events,
                file_id,
                args.stage,
                args.do_dbscan_clustering,
                args.ms_standard
            )
            cmd = con + f"{post_processing_cmd}'\""
            print(cmd)

            qsub_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)

    if args.stage == 2:
        file_id = -1
        post_processing_cmd = base_post_processing_cmd.format(
            rids_formatted,
            args.analysis_id,
            args.experiment_name,
            args.num_files_tracks,
            args.num_files_events,
            file_id,
            args.stage,
            args.do_dbscan_clustering,
            args.ms_standard
        )
        cmd = con + f"{post_processing_cmd}'\""
        print(cmd)

        qsub_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)

    # Done at the beginning and end of qsub main.
    #set_permissions()


def qsub_job(experiment_name, analysis_id, file_id, cmd, tlim):
    """
    ./qsub.py --job 'arbitrary command' [options]

    NOTE: redirecting the log file to a sub-folder is a little tricky
    https://stackoverflow.com/questions/15089315/redirect-output-to-different-files-for-sun-grid-engine-array-jobs-sge
    """
    qsub_opts = [
        "-S /bin/bash",  # use bash
        "-cwd",  # run from current working directory
        "-m n",  # don't send email notifications
        "-w e",  # verify syntax
        "-V",  # inherit environment variables
        f"-N {experiment_name}_aid_{analysis_id}_fid_{file_id}",  # job name
        f"-l h_rt={tlim}",  # time limit
        "-q all.q",  # queue name (cenpa only uses one queue)
        "-j yes",  # join stderr and stdout
        "-b y",  # Look for series of bytes.
        f"-o /data/eliza4/he6_cres/katydid_analysis/job_logs/post_processing/{experiment_name}_aid_{analysis_id}.txt",
    ]
    qsub_str = " ".join([str(s) for s in qsub_opts])
    batch_cmd = "qsub {} {}".format(qsub_str, cmd)

    print("running job")
    sp.run(batch_cmd, shell=True)

    return None


if __name__ == "__main__":
    main()
