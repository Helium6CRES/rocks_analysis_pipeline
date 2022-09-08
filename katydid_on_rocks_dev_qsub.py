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

pd.set_option('display.max_columns', 100)

def main():
    """
    """

    # need to source .bashrc for root to work properly
    # source = sp.run(['source /data/eliza4/he6_cres/.bashrc'], executable='/bin/bash')
    # Handling permissions issues when creating files/directories on rocks
    # sg doesn't work, takes user out of the script, umask has this behavior but shell=True got rid of it
    # sg = sp.run(['sg he6_cres'], executable='/bin/bash', shell=True)
    umask = sp.run(['umask u=rwx,g=rwx,o=rx'], executable='/bin/bash', shell=True)

    par = argparse.ArgumentParser()
    arg = par.add_argument
    arg('-id', '--run_id', type=int, help='run_id to run katydid on.')
    arg('-b','--base_config', type=str, help='base .yaml katydid config file to be run on run_id, should exist in base config directory.')
    arg('-fn', '--file_num', default=-1, type=int, help='Number of files in run id to analyze (<= number of files in run_id)')
    arg('-c', '--clean_up', default = False, type=bool, help = 'If true a clean_up run will be run.  ')
    args = par.parse_args()

    print(f"run_id: {args.run_id}")
    print(f"base_config: {args.base_config}")
   
    file_df = create_file_df(args.run_id)
    file_df["file_num"] = file_df.index
    file_df["rocks_file_path"] = file_df['file_path'].apply(lambda x: process_fp(x))
    file_df["exists"] = file_df["rocks_file_path"].apply(lambda x: check_if_exists(x))
    file_df["approx_slope"] = get_slope(file_df["true_field"][0] )

    dbscan_r = get_dbscan_radius(file_df["approx_slope"][0] )
    file_df["dbscan_radius_0"] = dbscan_r[0]
    file_df["dbscan_radius_1"] = dbscan_r[1]

    file_df["base_config_path"] = get_base_config_path(args.base_config)
    file_df["output_dir"] = build_dir_structure(args.run_id)

    # TODO: Change this to work.
    file_df["rocks_noise_file_path"] = file_df["rocks_file_path"]

    file_df["root_file_path"] = file_df.apply(lambda row: build_root_file_path(row), axis = 1)

    # print(file_df["root_file_path"][0])
    if args.file_num == -1:
        file_df.apply(lambda row: run_katydid(row), axis = 1)
    else:
        file_df[:args.file_num].apply(lambda row: run_katydid(row), axis = 1)

    #TODO: Write first copy of config file to root analysis dir. 


def run_katydid(file_df):


    base_config_path = Path(file_df["base_config_path"])

    # # Grab the config_dict from the katydid config file.
    with open(base_config_path, "r") as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    # Copy the katydid config file (in same dir) so that we can write to the copy not
    # the original.
    config_path = base_config_path.parent / str(
        base_config_path.stem + "_copy_" + str(file_df["run_id"]) + base_config_path.suffix
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
        ["/data/eliza4/he6_cres/katydid/build/bin/Katydid","-c" , config_path],
        capture_output=True
    )
    # print("\nspec file {} of {}".format(i + 1, len(spec_files)))
    print("Katydid output:", run_katydid.stdout[-100:])

    # Print statement to 
    now = datetime.datetime.now()
    print("file {file_df.index}. time: {}. root file {}".format(file_df["file_num"], now, file_df["rocks_file_path"]))

    # Delete the copy of the katydid config file once done with processing.
    Path(config_path).unlink()    

    return None

    # TODOS: 
    # * Currently we can make directories and make config files and verrrrrrry useful data frame columns. 
    # * Deal with group permissions. We need to be sure that root files that are built can be accessed/edited by all users in our group.
    # * Get katydid working in a place we can all use it. 
    # * Write first copy of config file to root analysis dir.
    # * WINSTON NEEDS TO FIGURE OUT PERMISSIONS!!!! Katydid isnt working for other users. JK
    # * make it work and get it running.
    # * Build out a way for people to interact with all these root files in a nice way. 
    # This is where the python post processing comes in and we pickle the file in rocks. 
    # Then anyone can open that pickle file and be on there way to working on an analysis.
    # * sp.run("chmod shit that deals with permissions WINSTON!!!!")


def build_root_file_path(file_df):
    root_path = Path(file_df["output_dir"]) / str(
        Path(file_df["rocks_file_path"]).stem + file_df["output_dir"][-4:] + ".root"
    )

    return str(root_path)

def get_base_config_path(base_config):

    base_path = Path('/data/eliza4/he6_cres/katydid_analysis/base_configs')
    base_config = base_path / Path(base_config)

    if not base_config.is_file():
        raise UserWarning("base config doesn't exist. ")

    return str(base_config)


def get_slope(true_field, frequency: float = 18.5e9 ): 

    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    approx_slope = sc.df_dt(approx_energy, true_field, approx_power)

    return approx_slope

def get_dbscan_radius(approx_slope: float, dbscan_base_radius: List[float]  = [5.0e-4, 40e6]) -> List[float]: 
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
    
    dbscan_radius = [dbscan_base_radius[1] / approx_slope , dbscan_base_radius[0] *approx_slope]

    return dbscan_radius



def run_id_to_slope(run_id: int, frequency: float = 18.0e9): 
    
    
    query = '''SELECT * FROM he6cres_runs.run_log
           WHERE run_log.run_id = {}
           ORDER BY run_id DESC LIMIT 10
            '''.format(run_id)
    
    run_id_info = db.he6cres_db_query(query)
    
    if not (len(run_id_info) == 1):
        raise ValueError(f'There seems to be no run with run_id = {run_id}.')
        
    true_field = run_id_info.true_field[0] 
    approx_power = sc.power_larmor(true_field, frequency)
    approx_energy = sc.freq_to_energy(frequency, true_field)
    approx_slope = sc.df_dt(approx_energy, true_field, approx_power)
    
    return approx_slope


def process_fp(daq_fp): 
    rocks_fp = '/data/eliza4/he6_cres/' + daq_fp[5:]
    return rocks_fp

def check_if_exists(rocks_fp): 
    rocks_fp = Path(rocks_fp)
    return rocks_fp.is_file()

def build_dir_structure(run_id): 

    base_path = Path('/data/eliza4/he6_cres/katydid_analysis/root_files')

    run_id_dir = base_path / Path(f"run_id_{run_id}") 

    if not run_id_dir.is_dir():
        run_id_dir.mkdir()
        print(f"Created directory: {run_id_dir}")

    analysis_dirs = glob(str(run_id_dir) + "/*/")
    analysis_index = len(analysis_dirs)
    current_analysis_dir = run_id_dir / Path(f"analysis_{analysis_index:03d}")
    if not current_analysis_dir.is_dir():
        current_analysis_dir.mkdir()
        print(f"Created directory: {current_analysis_dir}")

    return str(current_analysis_dir)


def create_file_df(run_id: int):
    """
    here we could set up a dataframe from the He6-CRES database
    """
    query_he6_db = '''
                    SELECT r.run_id, f.spec_id, f.file_path, r.true_field
                    FROM he6cres_runs.run_log as r
                    RIGHT JOIN he6cres_runs.spec_files as f
                    ON r.run_id = f.run_id
                    WHERE r.run_id = {}
                    ORDER BY r.created_at DESC
                  '''.format(run_id)
                  
    file_df = he6cres_db_query(query_he6_db)


    return  file_df


def run_chunk():
    """
    """
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
def he6cres_db_query(query: str ) -> typing.Union[None, pd.DataFrame]: 

    connection = False
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="chirality",
                                      host="wombat.npl.washington.edu",
                                      port="5544",
                                      database="he6cres_db")

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

if __name__=="__main__":
    main()
