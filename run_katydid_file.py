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
from rocks_utility import get_pst_time


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

    print(f"\nLoading file_df json. STARTING at PST time: {get_pst_time()}\n")

    # load file_df, then access a specific row.
    # TODO: possible to avoid having each job load the dataframe each time it spawns? I've had no luck passing individual rows around. Not sure if 1000 concurrent read_csv calls will be an issue.
    try:
        file_df_loaded = pd.read_json(args.file_df_json_path)
    except pd.errors.ParserError as e:
        print(f"Exception: Could not load file_df from {args.file_df_json_path}.")
        print(e)
        print("Returning.\n")
        return

    # Load the specific row by true file_id, not by positional index.
    matches = file_df_loaded.loc[file_df_loaded["file_id"].astype(int) == args.idx]
    print(f"\nLoading file_df json. DONE at PST time: {get_pst_time()}\n")

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

    # Grab the config_dict from the katydid config file.
    with open(base_config_path, "r") as f:
        try:
            config_dict = yaml.load(f, Loader=yaml.FullLoader)
            # print(config_dict)
        except yaml.YAMLError as e:
            print(e)

    # Copy the katydid config file (in same dir) so that we can write to the copy not
    # the original.
    rid = file_df_row["run_id"]
    aid = file_df_row["analysis_id"]
    fid = file_df_row["file_id"]

    temp_config_dir = base_config_path.parent / "temp"
    temp_config_dir.mkdir(exist_ok = True)

    config_path = temp_config_dir / str(
        base_config_path.stem
        + f"_{rid:04d}_{aid:03d}_{fid:04d}"
        + base_config_path.suffix
    )

    # copy base config file to edit
    copyfile(base_config_path, config_path)

    # Check the file extension of the first path in rocks_file_path list
    rocks_file_path = file_df_row["rocks_file_path"]
    first_rock_file = rocks_file_path[0] if rocks_file_path else ""
    if first_rock_file.endswith(".spec"):
        for processor in config_dict["processor-toolbox"]["processors"]:
            if processor["name"] == "spec2":
                processor["type"] = "spec-processor"
    elif first_rock_file.endswith(".speck"):
        for processor in config_dict["processor-toolbox"]["processors"]:
            if processor["name"] == "spec2":
                processor["type"] = "speck-processor"

    config_dict["spec1"]["filenames"] = file_df_row["rocks_noise_file_path"]
    config_dict["spec2"]["filenames"] = file_df_row["rocks_file_path"]

    for key, val in config_dict.items():
        for inner_key, inner_val in val.items():
            if inner_key == "output-file":
                config_dict[key][inner_key] = file_df_row["root_file_path"]

            if inner_key == "initial-slope":
                config_dict[key][inner_key] = file_df_row["approx_slope"]

            if inner_key == "min-slope":
                config_dict[key][inner_key] = file_df_row["approx_slope"] - 1e10
            
            #Keep the LTF acceptance area to 45 bins (90 Hz*s) but scale f vs t with slope based on good reconstruction at 0.711T and 2.00T
            #0.711T: frequency-acceptance: 3e5, time-gap-tolerance: 3.0e-4
            #2.00T: frequency-acceptance: 8e5, time-gap-tolerance: 1.0e-4
            k = 0.1597 * file_df_row["approx_slope"] + 9.88e8
            if k <= 0:
                raise ValueError("No real positive solution (k must be > 0)")

            if inner_key == "frequency-acceptance":
                config_dict[key][inner_key] = float(np.sqrt(90 * k))
            if inner_key == "time-gap-tolerance":
                config_dict[key][inner_key] = float(np.sqrt(90 / k))
            
            if inner_key == "radii":
                config_dict[key][inner_key] = [
                    file_df_row["dbscan_radius_0"],
                    file_df_row["dbscan_radius_1"],
                ]
    config_dict["stv"]["output-file"] = file_df_row["slew_file_path"]

    # Dump the altered config_dict into the copy of the config file.
    # Note that the comments are all lost because you only write the contents of the
    # config dict.
    with open(config_path, "w") as f:
        # Convert np types to pure Python types, otherwise yaml.dump will print binary for numpy types
        # This might be unnecessary after manually applying float(np.sqrt(...)) above, that was the main troublemaker
        config_dict = {k: (v.item() if isinstance(v, np.generic) else v) 
                       for k, v in config_dict.items()} # Todo: recursively explore config_dict and apply conversion only to lowest level items
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    # copy first config file to the analysis directory for future reference.
    if file_df_row["file_id"] == 0:
        config_path_name = base_config_path.stem + f"_{rid:04d}_{aid:03d}" + base_config_path.suffix
        analysis_dir = Path(file_df_row["root_file_path"]).parents[0]
        saved_config_path = analysis_dir / config_path_name
        copyfile(config_path, saved_config_path)

        print(
            f"Writing the config file used in analysis to disk here: \n {str(saved_config_path)}\n"
        )

    # Run katydid on the edited katydid config file.
    # Note that you need to have Katydid configured as a bash executable for this to
    # work (as is standard).
    t_start = time.process_time()
    cmd = [
        "/data/raid2/eliza4/he6_cres/katydid/build/bin/Katydid",
        "-c",
        str(config_path),
    ]

    proc = sp.run(cmd, capture_output=True, text=True, errors="replace")

    # Decode logs (avoid escape noise)
    print("Katydid stdout (tail 1k):", proc.stdout[-1000:], flush=True)
    if proc.stderr.strip():
        print("Katydid stderr (tail 1k):", proc.stderr[-1000:], flush=True)

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


if __name__ == "__main__":
    main()
