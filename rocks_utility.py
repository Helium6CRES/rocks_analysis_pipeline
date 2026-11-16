#!/usr/bin/env python3
import datetime
import fcntl
import hashlib
import os
import shutil
import subprocess as sp
import typing
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.io.sql as psql
import psycopg2
import pytz
from psycopg2 import Error

def he6cres_db_connection_local():

    # Connect to the he6cres_db from a machine on the CENPA vpn.
    connection = psycopg2.connect(
        user="postgres",
        password="chirality",
        host="192.168.20.80",
        port="5432",
        database="he6cres_db",
    )
    return connection


def he6cres_db_connection_rocks():

    # Connect to the he6cres_db from rocks.
    connection = psycopg2.connect(
        user="postgres",
        password="chirality",
        host="192.168.20.80",
        port="5432",
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

def sbatch_job(
        cmd: str, 
        job_name: str, 
        tlim: str, 
        log_path: typing.Union[str, Path], 
        array: typing.Union[int, str] = 0,
        max_concurrent: int = 0,
        cpus_per_task: int = 0,
        mem: int = 0,
        run_in_apptainer = False,
        hold: bool = False,
        ) -> sp.CompletedProcess:

    log_path = str(log_path)

    sbatch_cmd = [
        "sbatch",
        "--parsable",
        "--job-name", job_name,
        "--time", tlim,
        "--output", log_path,
        "--export=ALL",
        "--mail-type=NONE",
    ]

    if hold:
        sbatch_cmd.append("--hold")

    if array:
        if isinstance(array, int):
            if max_concurrent <= 0:
                max_concurrent = array
            sbatch_cmd.append(f"--array=0-{array-1}%{max_concurrent}")
        else:
            if max_concurrent > 0:
                sbatch_cmd.append(f"--array={array}%{max_concurrent}")
            else:
                sbatch_cmd.append(f"--array={array}")
    if cpus_per_task > 0:
        sbatch_cmd.append(f"--cpus-per-task={cpus_per_task}")

    if mem > 0:
        sbatch_cmd.append(f"--mem={mem}G")

    if run_in_apptainer:
        # Build the command to run inside the container
        container_cmd = (
            f"umask 002; "
            f"source /data/raid2/eliza4/he6_cres/.bashrc; "
        )

        # Full Apptainer command
        # Wrapping cmd in single quotes means literal text: no variable expansion etc
        full_cmd = (
            "apptainer exec "
            "--bind /data/raid2/eliza4/he6_cres/:/data/raid2/eliza4/he6_cres/,"
            "/data/raid4/he6cres:/data/raid4/he6cres "
            "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
            f"/bin/bash -c '{container_cmd}{cmd}'"
        )
    else:
        full_cmd = cmd

    print("\n\n", " ".join(sbatch_cmd + [full_cmd]), "\n\n")

    script = f"""#!/bin/bash
    set -euo pipefail
    {full_cmd}
    """

    proc = sp.run(
        sbatch_cmd, 
        input = script, 
        check=True,
        text=True, 
        capture_output=hold, 
    )
    return proc

def node_local_cached_file(source, cache_dir = "/tmp", unique = False):
    """
    Get a path to a local cache of some source file on node-local storage
    (typically node-local /tmp or /scratch) with a lockfile to prevent race
    conditions.
    By default, caches in cache_dir="/tmp". Pass unique=True to add a hash
    to uniquely identify different files that might have the same name
    Returns cache file Path
    """
    source = Path(source).resolve()
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    local_stem = source.stem
    suffix = source.suffix
    if unique:
        # Avoid collisions between files with identical basenames
        key = hashlib.sha256(str(source).encode()).hexdigest()[:16]
        local_stem = f"{source.stem}_{key}"

    local_file = cache_dir / f"{local_stem}{suffix}"
    lock_file = cache_dir / f"{local_stem}.lock"

    with open(lock_file, "w") as lock:
        # Only one job per node can stage this file at a time
        fcntl.flock(lock, fcntl.LOCK_EX)

        if local_file.exists():
            fcntl.flock(lock, fcntl.LOCK_UN)
            print(f"Found local cache of {source}")
            return local_file

        if not local_file.exists():
            temp_file = cache_dir / f"{local_stem}.tmp.{os.getpid()}{suffix}"
            try:
                print(f"Copying {source} to local scratch")
                # python >=3.14: replace shutil with Path.copy(target)
                shutil.copy2(source, temp_file)
                os.replace(temp_file, local_file)
            finally:
                if temp_file.exists():
                    temp_file.unlink()
        fcntl.flock(lock, fcntl.LOCK_UN)

    if local_file.exists():
        return local_file
    else:
        print("Caching failed! Returning original source path.")
        return source

