#!/usr/bin/env python3
import psycopg2
from psycopg2 import Error
import pandas as pd
import pytz
import numpy as np
import datetime
import typing
import pandas.io.sql as psql
import subprocess as sp
from pathlib import Path


def he6cres_db_connection_local():

    # Connect to the he6cres_db from a machine on the CENPA vpn.
    connection = psycopg2.connect(
        user="postgres",
        password="chirality",
        host="10.66.192.47",
        port="5432",
        database="he6cres_db",
    )
    return connection


def he6cres_db_connection_rocks():

    # Connect to the he6cres_db from rocks.
    connection = psycopg2.connect(
        user="postgres",
        password="chirality",
        host="wombat.npl.washington.edu",
        port="5544",
        database="he6cres_db",
    )
    return connection


def he6cres_db_query(query: str, local=False) -> typing.Union[None, pd.DataFrame]:

    try:
        # Create a connection to the database
        if not local:
            with he6cres_db_connection_rocks() as connection:
                with connection.cursor() as cursor:
                    # Execute a SQL command
                    cursor.execute(query)
                    cols = [desc[0] for desc in cursor.description]
                    query_result = pd.DataFrame(cursor.fetchall(), columns=cols)
        else:
            with he6cres_db_connection_local() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    cols = [desc[0] for desc in cursor.description]
                    query_result = pd.DataFrame(cursor.fetchall(), columns=cols)

    except (Exception, Error) as error:
        print("Error while connecting to he6cres_db", error)
        query_result = None

    return query_result


def get_pst_time():
    tz = pytz.timezone("US/Pacific")
    pst_now = datetime.datetime.now(tz).replace(microsecond=0).replace(tzinfo=None)
    return pst_now


def set_permissions():
    """
    Note that this is necessary for the output (files and dirs) of the analysis to have
    the right permissions so that all other group members can also run an analysis. The
    issue is that since not all files in katydid_analysis are owned by any one of us,
    we will all get long error messages written to our job logs (for each file we don't
    own). The below still works well but the output is supressed for this reason. In the
    future we may want a more targeted command (change permissions for all files I own).
    For now this works.

    NOTE: I think this function is taking a lot of time in our submissions. We should 
    limit it's use whenever possible. 
    """
    timeout_seconds = 30

    try:
        cmd1 = "chgrp -R he6_cres katydid_analysis/ >/dev/null 2>&1"
        sp.call(cmd1, shell=True, timeout=timeout_seconds)

        cmd2 = "chmod -R 774 katydid_analysis/ >/dev/null 2>&1"
        sp.call(cmd2, shell=True, timeout=timeout_seconds)

    except Exception:
        print("set_permissions() function in rocks_utility.py failed.")

    return None


def check_if_exists(fp_input):
    # Check if the input is a list or a single path
    if isinstance(fp_input, list):
        return all(Path(fp).is_file() for fp in fp_input)
    else:
        return Path(fp_input).is_file()

def log_file_break():
    print("\n\n")
    print("################################################################")
    print("\n\n")
    return None
