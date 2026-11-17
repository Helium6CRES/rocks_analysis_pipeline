import sys
import argparse
import time
from pathlib import Path
from shutil import copyfile
import yaml
import subprocess as sp
import pandas as pd
import numpy as np

# Local imports
from rocks_utility import get_pst_time, node_local_cached_file


# Import settings
pd.set_option("display.max_columns", 100)


def main() -> None:
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("--file_df_json_path", type=str,
        help="Path to file_df json. Read in as pandas dataframe")
    arg("-id", "--idx", type=int, 
        help="Integer index of row to load from file_df.")

    args = par.parse_args()

    # ----------------------------------------------------------------------
    # Load file_df from disk
    # ----------------------------------------------------------------------
    # TODO: possible to avoid having each job load the dataframe each time it spawns? 
    # I've had no luck passing individual rows around. 
    # Not sure if 1000 concurrent read_csv calls is an issue.
    print(f"\nLoading file_df json. STARTING at PST time: {get_pst_time()}\n", flush = True)

    try:
        file_df_loaded = pd.read_json(args.file_df_json_path)
    except pd.errors.ParserError as e:
        print(f"Exception: Could not load file_df from {args.file_df_json_path}.")
        print(e)
        print("Returning.\n")
        return

    print(f"\nLoading file_df json. DONE at PST time: {get_pst_time()}\n", flush = True)

    # ----------------------------------------------------------------------
    # Load the specific row by true file_id, not by positional index.
    # ----------------------------------------------------------------------
    matches = file_df_loaded.loc[file_df_loaded["file_id"].astype(int) == args.idx]

    if matches.empty:
        print(f"Exception: file_id {args.idx} not found in {args.file_df_json_path}.")
        print("Returning.\n")
        return

    if len(matches) > 1:
        print(f"Exception: file_id {args.idx} is duplicated in {args.file_df_json_path}.")
        print(matches.to_string())
        print("Returning.\n")
        return

    file_df_row = matches.iloc[0].to_dict()

    # Force a write to the log.
    sys.stdout.flush()
    try:
        print(
            f"\nProcessing file_id {file_df_row['file_id']}. STARTING at PST time: {get_pst_time()}",
            flush=True
        )
        run_katydid_file(file_df_row)
        print(
            f"Processing file_id {file_df_row['file_id']}. DONE at PST time: {get_pst_time()}",
            flush=True
        )
    except Exception as e:
        print(
            f"Exception while processing file_id {file_df_row['file_id']}: {e}",
            flush=True
        )


def run_katydid_file(file_df_row: dict) -> None:
    """
    Runs katydid on a single file based on configuration data in a specific row of a file_df

    Parameters
    ----------
    file_df_row: dict
        Loaded from a pd.Series of a file_df row. 
        Keys used:
          run_id, file_id, analysis_id,
          base_config_path, rocks_file_path, rocks_noise_file_path, root_file_path, slew_file_path,
          approx_slope, dbscan_radius_0, dbscan_radius_1
    """

    base_config_path = Path(file_df_row["base_config_path"])
    rid = file_df_row["run_id"]
    aid = file_df_row["analysis_id"]
    fid = file_df_row["file_id"]

    rocks_noise_path = file_df_row["rocks_noise_path"]
    rocks_file_path = file_df_row["rocks_file_path"]
    root_file_path = file_df_row["root_file_path"]
    slew_file_path = file_df_row["slew_file_path"]

    approx_slope = file_df_row["approx_slope"]
    dbscan_radius_0 = file_df_row["dbscan_radius_0"]
    dbscan_radius_1 = file_df_row["dbscan_radius_1"]

    use_cache = True
    if use_cache:
        cached_noise_path = [str(cache_file_in_node(f)) for f in rocks_noise_path]
        cached_file_path = [str(cache_file_in_node(f)) for f in rocks_file_path]

    #Keep the LTF acceptance area to 90 Hz*s (45 bins with ROACH avg = 2)
    #but scale f vs t with slope based on good reconstruction at 0.711T and 2.00T
    # - 0.71T: frequency-acceptance: 3e5, time-gap-tolerance: 3.0e-4
    # - 2.00T: frequency-acceptance: 8e5, time-gap-tolerance: 1.0e-4
    k = 0.1597 * approx_slope + 9.88e8
    if k <= 0:
        raise ValueError("No real positive solution (k must be > 0)")
    LTF_size = 90 # Hz*s = # bins * ROACH_avg
    LTF_freq_acceptance = float(np.sqrt(LTF_size_bins * k))
    LTF_time_acceptance = float(np.sqrt(LTF_size_bins / k))

    # Copy the katydid config file (in same dir) so that we can write to the copy not
    # the original.
    # copy base config file to edit
    temp_config_dir = base_config_path.parent / "temp"
    temp_config_dir.mkdir(exist_ok = True)
    config_path = temp_config_dir / str(
        base_config_path.stem
        + f"_{rid:04d}_{aid:03d}_{fid:04d}"
        + base_config_path.suffix
    )
    copyfile(base_config_path, config_path)

    # Grab the config_dict from the katydid config file.
    config_dict = load_yaml(config_path)
    # print(config_dict)

    # configure spec2 to be either a spec-processor or speck-processor
    # Check the file extension of the first path in rocks_file_path list
    data_file_suffix = Path(rocks_file_path[0]).suffix if rocks_file_path else ""
    processor_type = {
        ".spec": "spec-processor",
        ".speck": "speck-processor",
    }.get(data_file_suffix) # returns None if missing key
    if processor_type:
        for processor in config_dict["processor-toolbox"]["processors"]:
            if processor["name"] == "spec2":
                processor["type"] = processor_type

    inner_key_replace = {
        "output-file": root_file_path,
        "initial-slope": approx_slope,
        "min-slope": approx_slope - 1e10,
        "frequency-acceptance": LTF_freq_acceptance,
        "time-gap-tolerance": LTF_time_acceptance,
        "radii": [dbscan_radius_0, dbscan_radius_1],
    }
    for outer_key, val in config_dict.items():
        for inner_key in val.keys():
            new_value = inner_key_replace.get(inner_key)
            if new_value:
                config_dict[outer_key][inner_key] = new_value

    config_dict["spec1"]["filenames"] = rocks_noise_path
    config_dict["spec2"]["filenames"] = rocks_file_path
    config_dict["stv"]["output-file"] = slew_file_path

    # This might be unnecessary after manually applying float(np.sqrt(...)) above, that was the main troublemaker
    config_dict = sanitize_dict(config_dict)
    # Dump the altered config_dict into the copy of the config file.
    # Note that the comments are all lost because you only write the contents of the config dict.
    with open(config_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    # copy first config file to the analysis directory for future reference.
    if fid == 0:
        config_path_name = base_config_path.stem + f"_{rid:04d}_{aid:03d}" + base_config_path.suffix
        analysis_dir = Path(root_file_path).parents[0]
        saved_config_path = analysis_dir / config_path_name
        copyfile(config_path, saved_config_path)

        print(
            f"Writing the config file used in analysis to disk here: \n {str(saved_config_path)}\n"
        )

    if use_cache:
        config_dict["spec1"]["filenames"] = cached_noise_path
        config_dict["spec2"]["filenames"] = cached_file_path
        cached_config_path = "/tmp/he6cres/" + config_path.name
        with open(cached_config_path, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
        
    config_path_str = str(cached_config_path if use_cache else config_path)

    # Run katydid on the edited katydid config file.
    # Note that you need to have Katydid configured as a bash executable for this to
    # work (as is standard).
    dt_start = get_pst_time()
    t_start = time.process_time()

    cmd = [
        "/data/raid2/eliza4/he6_cres/katydid/build/bin/Katydid",
        "-c",
        config_path_str,
    ]

    proc = sp.run(cmd, capture_output=True, text=True, errors="replace")

    # Decode logs (avoid escape noise)
    print("Katydid stdout (tail 1k):", proc.stdout[-1000:], flush=True)
    if proc.stderr.strip():
        print("Katydid stderr (tail 1k):", proc.stderr[-1000:], flush=True)

    t_stop = time.process_time()
    t_elapsed = t_stop - t_start
    dt_stop = get_pst_time()
    dt_elapsed = dt_stop - dt_start

    root_exists = root_file_path.is_file()
    root_size = root_file_path.stat().st_size if root_exists else 0

    if use_cache:
        cached_config_path.unlink()
        # DO NOT try and delete cached .spec(k) files unless there is a serious issue with disk usage
        # in the temp storage. If you mishandle the paths you could accidentally delete the original data file!
        # If you absolutely must, be extremely careful and at minimum do a check like:
        # (not str(path.resolve()).startswith("/data")) and str(path.resolve()).startswith("/tmp/")

    # Only claim success if returncode==0 and the file exists and is non-empty
    if proc.returncode == 0 and root_exists and root_size > 0:
        print(
            f"\nfile {fid}."
            f"\ncurrent wall time: {dt_stop}."
            f"\nwall time elapsed: {dt_elapsed}."
            f"\nCPU time elapsed: {t_elapsed:.2f} s."
            f"\nroot file created {root_path}\n"
        )
        config_path.unlink()

    else:
        print(
            f"\nfile {fid} FAILED."
            f"\nwall time elapsed: {dt_elapsed}."
            f"\nCPU time elapsed: {t_elapsed:.2f} s."
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

def load_yaml(yaml_path):
    with open(yaml_path, "r") as f:
        try:
            return yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as e:
            print(e)
    return {}

def sanitize_dict(d):
    """
    Convert numpy types to pure Python types, otherwise yaml.dump will print binary for numpy types
    """
    # TODO: recursively explore config_dict and apply conversion only to lowest level items
    return {k: (v.item() if isinstance(v, np.generic) else v)
                   for k, v in d.items()}

if __name__ == "__main__":
    main()
