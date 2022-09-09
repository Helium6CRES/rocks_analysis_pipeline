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

# Local imports: 
from .run_katydid import build_file_df_path, check_if_exists

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
        type=int,
        help="list of run_ids to collect track data for.",
    )
    arg(
        "-aid",
        "--analysis_id",
        type=int,
        help="analysis_id to collect track data for.",
    )

    args = par.parse_args()


    # Sanity check: 
    print("TEST", args.run_ids, args.analysis_id)

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
            print("Analysis Type: Clean up.")

            file_df = pd.read_csv(file_df_path)
            file_df["root_file_exists"] = file_df["root_file_path"].apply(
                lambda x: check_if_exists(x)
            )
            file_df.append(file_df_list)

        # New analysis.
        else:
            raise UserWarning(f"One of the listed run_ids has no analysis_id = {analysis_id}")


    file_df_experiment = df = pd.concat(file_df_list)

    print(len(file_df_experiment))
    print(file_df_experiment)


def set_permissions():

    set_group = sp.run(["chgrp", "-R", "he6_cres", "katydid_analysis/"])
    set_permission = sp.run(["chmod", "-R", "774", "katydid_analysis/"])

    return None


if __name__ == "__main__":
    main()
