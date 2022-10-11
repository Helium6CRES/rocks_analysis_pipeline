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


def he6cres_db_query(query: str) -> typing.Union[None, pd.DataFrame]:

    connection = False
    try:
        # Connect to an existing database
        connection = psycopg2.connect(
            user="postgres",
            password="chirality",
            host="wombat.npl.washington.edu",
            port="5544",
            database="he6cres_db",
        )

        # Create a cursor to perform database operations
        cursor = connection.cursor()

        # Execute a sql_command
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        query_result = pd.DataFrame(cursor.fetchall(), columns=cols)

    except (Exception, Error) as error:
        print("Error while connecting to he6cres_db", error)
        query_result = None

    finally:
        if connection:
            cursor.close()
            connection.close()

    return query_result


def get_pst_time():
    tz = pytz.timezone("US/Pacific")
    pst_now = datetime.datetime.now(tz).replace(microsecond=0).replace(tzinfo=None)
    return pst_now


def set_permissions():

    set_group = sp.run(["chgrp", "-R", "he6_cres", "katydid_analysis/"])
    set_permission = sp.run(["chmod", "-R", "774", "katydid_analysis/"])

    return None


def check_if_exists(fp):
    return Path(fp).is_file()

def log_file_break(): 
    print("\n\n")
    print("################################################################")
    print("\n\n")
    return None

