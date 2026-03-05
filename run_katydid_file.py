import sys
import time
from pathlib import Path
from shutil import copyfile
import yaml
import subprocess as sp
import pandas as pd

# Local imports
from rocks_utility import (
        get_pst_time,
    )

# Import settings
pd.set_option("display.max_columns", 100)

def run_katydid_file(file_df_row: pd.Series):
    """
    Runs katydid on a single file based on configuration data in a specific row of a file_df

    Parameters
    ----------
    file_df_row: pd.Series
        Columns used: 
          run_id, file_id, analysis_id, 
          base_config_path, rocks_file_path, rocks_noise_file_path, root_file_path, slew_file_path, 
          approx_slope, dbscan_radius_0, dbscan_radius_1
    """

    # Force a write to the log.
    sys.stdout.flush()

    base_config_path = Path(file_df_row["base_config_path"])
    
    # Grab the config_dict from the katydid config file.
    with open(base_config_path, "r") as f:
        try:
            config_dict = yaml.load(f, Loader=yaml.FullLoader)
            #print(config_dict)
        except yaml.YAMLError as e:
            print(e)

    # Copy the katydid config file (in same dir) so that we can write to the copy not
    # the original.
    rid = file_df_row["run_id"]
    aid = file_df_row["analysis_id"]

    config_path = base_config_path.parent / str(
        base_config_path.stem + f"_{rid:04d}_{aid:03d}" + base_config_path.suffix
    )

    # copy base config file to edit
    copyfile(base_config_path, config_path)

    # Check the file extension of the first path in rocks_file_path list
    rocks_file_path = file_df_row["rocks_file_path"]
    first_rock_file = rocks_file_path[0] if rocks_file_path else ""
    if first_rock_file.endswith(".spec"):
        for processor in config_dict['processor-toolbox']['processors']:
            if processor['name'] == 'spec2':
                processor['type'] = 'spec-processor'
    elif first_rock_file.endswith(".speck"):
        for processor in config_dict['processor-toolbox']['processors']:
            if processor['name'] == 'spec2':
                processor['type'] = 'speck-processor'

    config_dict["spec1"]["filenames"] = file_df_row["rocks_noise_file_path"]
    config_dict["spec2"]["filenames"] = file_df_row["rocks_file_path"]

    for key, val in config_dict.items():
        for inner_key, inner_val in val.items():
            if inner_key == "output-file":
                config_dict[key][inner_key] = file_df_row["root_file_path"]

            if inner_key == "initial-slope":
                config_dict[key][inner_key] = file_df_row["approx_slope"]

            if inner_key == "min-slope":
                config_dict[key][inner_key] = file_df_row["approx_slope"]-1e10

            if inner_key == "radii":
                config_dict[key][inner_key] = [
                    file_df_row["dbscan_radius_0"],
                    file_df_row["dbscan_radius_1"],
                ]
    config_dict["stv"]["output-file"] = file_df_row["slew_file_path"]

    # Dump the altered config_dict into the copy of the config file.
    # Note that the comments are all lost because you only write the contents of the
    # confic dict.
    with open(config_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    
    # copy first config file to the analysis directory for future reference.
    if file_df_row["file_id"] == 0:
        analysis_dir = Path(file_df_row["root_file_path"]).parents[0]
        config_path_name = Path(config_path).name
        saved_config_path = analysis_dir / config_path_name
        copyfile(config_path, saved_config_path)

        print(
            f"Writing the config file used in analysis to disk here: \n {str(saved_config_path)}\n"
        )

    # Run katydid on the edited katydid config file.
    # Note that you need to have Katydid configured as a bash executable for this to
    # work (as is standard).
    t_start = time.process_time()
    proc = sp.run(
        ["/data/raid2/eliza4/he6_cres/katydid/build/bin/Katydid", "-c", str(config_path)],
        capture_output=True,
    )

    # Decode logs (avoid escape noise)
    out = proc.stdout.decode(errors="replace")
    err = proc.stderr.decode(errors="replace")

    print("Katydid stdout (tail 1k):", out[-1000:])
    if err.strip():
        print("Katydid stderr (tail 1k):", err[-1000:])

    t_stop = time.process_time()
    elapsed = t_stop - t_start

    root_path = Path(file_df_row["root_file_path"])
    root_exists = root_path.is_file()
    root_size = root_path.stat().st_size if root_exists else 0

    # Only claim success if returncode==0 and the file exists and is non-empty
    if proc.returncode == 0 and root_exists and root_size > 0:
        print(
            f"\nfile {file_df_row['file_id']}."
            f"\ntime to run: {elapsed:.2f} s."
            f"\ncurrent time: {get_pst_time()}."
            f"\nroot file created {root_path}\n"
        )
        # Safe to remove the temp config
        if Path(config_path).exists():
            Path(config_path).unlink()

    else:
        print(
            f"\nfile {file_df_row['file_id']} FAILED."
            f"\ntime: {elapsed:.2f} s."
            f"\nreturncode: {proc.returncode}"
            f"\nroot exists: {root_exists} size: {root_size}\n"
            f"Config kept for debug: {config_path}\n"
        )
        # Optionally rename to mark failure
        fail_cfg = config_path.with_suffix(config_path.suffix + ".failed")
        try:
            config_path.rename(fail_cfg)
            print(f"Saved failing config as: {fail_cfg}")
        except Exception as e:
            print(f"Could not rename failing config: {e}")
        # Re-raise or just return; your choice:
        # raise RuntimeError("Katydid failed")

    return None



