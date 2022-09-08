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
import he6_cres_spec_sims.spec_tools.spec_calc.spec_calc as sc

pd.set_option("display.max_columns", 100)


def main():
    """

    TODOS:
    * Need to source .bashrc for root to work properly. WORKS
    * Need to have the permissions such that the folders are writeable by others in
    our group.
    * Currently we can make directories and make config files and verrrrrrry useful data frame columns.
    * Deal with group permissions. We need to be sure that root files that are built can be accessed/edited by all users in our group.
    * Get katydid working in a place we can all use it.
    * Write first copy of config file to root analysis dir.
    * WINSTON NEEDS TO FIGURE OUT PERMISSIONS!!!! Katydid isnt working for other users. JK
    * make it work and get it running.
    * Build out a way for people to interact with all these root files in a nice way.
    This is where the python post processing comes in and we pickle the file in rocks.
    Then anyone can open that pickle file and be on there way to working on an analysis.
    * sp.run("chmod shit that deals with permissions WINSTON!!!!")
    * Write first copy of config file to root analysis dir.

    """

    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument
    arg("-id", "--run_id", type=int, help="run_id to run katydid on.")
    arg(
        "-ai",
        "--analysis_index",
        type=int,
        help="analysis_index used to label directories.",
    )
    arg(
        "-b",
        "--base_config",
        type=str,
        help="base .yaml katydid config file to be run on run_id, should exist in base config directory.",
    )
    arg(
        "-fn",
        "--file_num",
        default=-1,
        type=int,
        help="Number of files in run id to analyze (<= number of files in run_id)",
    )
    arg(
        "-c",
        "--clean_up",
        default=False,
        type=bool,
        help="If true a clean_up run will be run on given analysis_index.",
    )
    args = par.parse_args()

    print(f"run_id: {args.run_id}")
    print(f"base_config: {args.base_config}")

    # New idea: If the file_df exists then it is a clean-up run:
    file_df_path = build_file_df_path(args.run_id, args.analysis_index)
    print(f"\nfile_df_path: {file_df_path}. exists: {file_df_path.is_file()}\n")

    if file_df_path.is_file():

        file_df = pd.read(file_df_path)
        file_df["root_file_exists"] = file_df["root_file_path"].apply(
            lambda x: check_if_exists(x)
        )

    else:
        file_df = create_file_df(args.run_id)
        file_df["root_file_exists"] = False
        file_df["file_num"] = file_df.index
        file_df["rocks_file_path"] = file_df["file_path"].apply(lambda x: process_fp(x))
        file_df["exists"] = file_df["rocks_file_path"].apply(
            lambda x: check_if_exists(x)
        )
        file_df["approx_slope"] = get_slope(file_df["true_field"][0])

        dbscan_r = get_dbscan_radius(file_df["approx_slope"][0])
        file_df["dbscan_radius_0"] = dbscan_r[0]
        file_df["dbscan_radius_1"] = dbscan_r[1]

        file_df["base_config_path"] = get_base_config_path(args.base_config)
        file_df["output_dir"] = build_dir_structure(args.run_id, args.analysis_index)

        # TODO: Change this to work.
        file_df["rocks_noise_file_path"] = file_df["rocks_file_path"]

        file_df["root_file_path"] = file_df.apply(
            lambda row: build_root_file_path(row), axis=1
        )

        # Trim the df according to the file_num arg.
        if args.file_num != -1:
            file_df = file_df[: args.file_num]

        # Before running katydid write this df to the analysis dir.
        # This will be used during the cleanup
        file_df_path = build_file_df_path(args.run_id, args.analysis_index)
        print(f"file_df_path: {file_df_path}")
        file_df.to_csv(file_df_path)

    condition = file_df["root_file_exists"] != True

    print(f"\nRunning katydid on {condition.sum()} of {len(file_df)} files.")
    # Run katydid on each row/spec file in file_df.
    file_df[condition].apply(lambda row: run_katydid(row), axis=1)


def run_katydid(file_df):

    base_config_path = Path(file_df["base_config_path"])

    # # Grab the config_dict from the katydid config file.
    with open(base_config_path, "r") as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    # Copy the katydid config file (in same dir) so that we can write to the copy not
    # the original.
    config_path = base_config_path.parent / str(
        base_config_path.stem
        + "_copy_"
        + str(file_df["run_id"])
        + base_config_path.suffix
    )
    copyfile(base_config_path, config_path)
    print(config_path)

    # TODO: input noise file path.
    config_dict["spec1"]["filename"] = file_df["rocks_noise_file_path"]
    config_dict["spec2"]["filename"] = file_df["rocks_file_path"]

    for key, val in config_dict.items():
        for inner_key, inner_val in val.items():
            if inner_key == "output-file":
                config_dict[key][inner_key] = file_df["root_file_path"]

            if inner_key == "initial-slope":

                config_dict[key][inner_key] = file_df["approx_slope"]

            if inner_key == "radii":
                config_dict[key][inner_key] = [
                    file_df["dbscan_radius_0"],
                    file_df["dbscan_radius_1"],
                ]

    # Dump the altered config_dict into the copy of the config file.
    # Note that the comments are all lost because you only write the contents of the
    # confic dict.
    with open(config_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    # Run katydid on the edited katydid config file.
    # Note that you need to have Katydid configured as a bash executable for this to
    # work (as is standard).
    run_katydid = sp.run(
        ["/data/eliza4/he6_cres/katydid/build/bin/Katydid", "-c", config_path],
        capture_output=True,
    )
    # print("\nspec file {} of {}".format(i + 1, len(spec_files)))
    print("Katydid output:", run_katydid.stdout[-100:])

    # Print statement to
    now = datetime.datetime.now()
    print(
        "\nfile {}. time: {}. root file created {}\n".format(
            file_df["file_num"], now, file_df["root_file_path"]
        )
    )

    # Delete the copy of the katydid config file once done with processing.
    Path(config_path).unlink()

    return None


def root_file_check(file_df):
    return Path(file_df["root_file_path"]).exists()


def build_file_df_path(run_id, analysis_index):
    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")
    rid_ai_dir = base_path / Path(f"rid_{run_id:04d}") / Path(f"ai_{analysis_index:03d}")

    file_df_path = rid_ai_dir / Path(
        f"rid_{run_id}_{analysis_index}.csv"
    )
    return file_df_path


def build_root_file_path(file_df):
    root_path = Path(file_df["output_dir"]) / str(
        Path(file_df["rocks_file_path"]).stem + file_df["output_dir"][-4:] + ".root"
    )

    return str(root_path)


def get_base_config_path(base_config):

    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/base_configs")
    base_config = base_path / Path(base_config)

    if not base_config.is_file():
        raise UserWarning("base config doesn't exist. ")

    return str(base_config)


def get_slope(true_field, frequency: float = 18.5e9):

    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

    return approx_slope


def get_dbscan_radius(
    approx_slope: float, dbscan_base_radius: List[float] = [5.0e-4, 40e6]
) -> List[float]:
    """
    Parameters
    ----------
    slope : float
        The slope of the tracks.
    dbscan_base_radii : List[float]
        Two elememnt list corresponding to the ideal dbscan radius for 8e10 Hz/s.
        Ideally this wouldn't be a hardcoded requirement but not sure how else to do it.

    Returns
    ------
    dbscan_radii : List[float]
        Two elememnt list corresponding to the ideal dbscan radii for given slope.

    """

    dbscan_radius = [
        dbscan_base_radius[1] / approx_slope,
        dbscan_base_radius[0] * approx_slope,
    ]

    return dbscan_radius


def run_id_to_slope(run_id: int, frequency: float = 18.0e9):

    query = """SELECT * FROM he6cres_runs.run_log
           WHERE run_log.run_id = {}
           ORDER BY run_id DESC LIMIT 10
            """.format(
        run_id
    )

    run_id_info = db.he6cres_db_query(query)

    if not (len(run_id_info) == 1):
        raise ValueError(f"There seems to be no run with run_id = {run_id}.")

    true_field = run_id_info.true_field[0]
    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

    return approx_slope


def process_fp(daq_fp):
    rocks_fp = "/data/eliza4/he6_cres/" + daq_fp[5:]
    return rocks_fp


def check_if_exists(fp):
    return Path(fp).is_file()


def build_dir_structure(run_id, analysis_index):

    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")

    run_id_dir = base_path / Path(f"rid_{run_id:04d}")

    if not run_id_dir.is_dir():
        raise UserWarning("This directory should have been made already.")

    current_analysis_dir = run_id_dir / Path(f"ai_{analysis_index:03d}")
    if not current_analysis_dir.is_dir():
        current_analysis_dir.mkdir()
        print(f"Created directory: {current_analysis_dir}")

    return str(current_analysis_dir)


def create_file_df(run_id: int):
    """
    here we could set up a dataframe from the He6-CRES database
    """
    query_he6_db = """
                    SELECT r.run_id, f.spec_id, f.file_path, r.true_field
                    FROM he6cres_runs.run_log as r
                    RIGHT JOIN he6cres_runs.spec_files as f
                    ON r.run_id = f.run_id
                    WHERE r.run_id = {}
                    ORDER BY r.created_at DESC
                  """.format(
        run_id
    )

    file_df = he6cres_db_query(query_he6_db)

    return file_df


def run_chunk():
    """ """
    pass

    # run_id = 1 # pass this in

    # # load your input dataframe
    # f_list = glob('/data/eliza4/he6_cres/**', recursive=True)

    # df = pd.DataFrame(f_list, columns=['fname', 'config_file'])

    # # select the chunk to process
    # df_chunk = df.query(f'chunk_idx == {args.chunk_index}').copy()
    # # df['newcol'] =

    # # take the first line & build the katydid command string
    # # figure out proc speed in files/min, GB/min

    # # t_start = time.time()
    # # run thing
    # # t_stop = time.time()

    # def run_katydid(df_row):
    #     print(df_row)
    #     # katydid_cmd = f"katydid {df_row.file} + {df_row.config}"
    #     # sp.call(katydid_cmd, shell=True)
    #     exit()

    # df_chunk.apply(run_katydid, axis=1)


# Simplify to not have an insert capability.
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
        print("Successfully connected to he6cres_db")

        # Execute a sql_command
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        query_result = pd.DataFrame(cursor.fetchall(), columns=cols)

        print("Query executed.")

    except (Exception, Error) as error:
        print("Error while connecting to he6cres_db", error)
        query_result = None

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection to he6cres_db is closed")

    return query_result


if __name__ == "__main__":
    main()
