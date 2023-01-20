#!/usr/bin/env python3
import subprocess as sp
import argparse
from typing import List
from pathlib import Path
from glob import glob


def main():
    """
    DOCUMENT
    """
    par = argparse.ArgumentParser()
    arg, st = par.add_argument, "store_true"
    # arg('--job', nargs=1, type=str, help='command to execute, usually in quotes')
    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--runids", nargs="+", type=int, help="run ids to analyze")
    arg(
        "-nid",
        "--noise_run_id",
        type=int,
        help="run_id to use for noise floor in katydid run.",
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
        "-aid",
        "--analysis_id",
        type=int,
        default=-1,
        help="analysis_id used to label directories. If -1, a new index will be created. \
        If doing a clean then WHAT??",
    )

    args = par.parse_args()

    tlim = "12:00:00" if args.tlim is None else args.tlim[0]

    # If the analysis_id is set to -1 then a new directory is built.
    # Else you will conduct a clean-up.
    if args.analysis_id == -1:
        # Get the analysis index to use for the list of jobs.
        analysis_id = get_analysis_id(args.runids)
        print(f"analysis_id: {analysis_id}")
    else:
        analysis_id = args.analysis_id

    # Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.
    con = "\"singularity exec --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif /bin/bash -c $'source /data/eliza4/he6_cres/.bashrc {} ".format(
        r"\n"
    )

    for run_id in args.runids:
        default_katydid_sub = 'python3 /data/eliza4/he6_cres/rocks_analysis_pipeline/run_katydid.py -id {} -nid {} -ai {} -b "{}" -fn {} '.format(
            run_id, args.noise_run_id, analysis_id, args.base_config, args.file_num
        )
        cmd = con + f"{default_katydid_sub}'\""
        qsub_job(run_id, analysis_id, cmd, tlim)


def qsub_job(run_id, analysis_id, cmd, tlim):
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
        f"-N run_id_{run_id}_{analysis_id}",  # job name
        f"-l h_rt={tlim}",  # time limit
        "-q all.q",  # queue name (cenpa only uses one queue)
        "-j yes",  # join stderr and stdout
        "-b y",  # Look for series of bytes.
        f"-o /data/eliza4/he6_cres/katydid_analysis/job_logs/katydid/rid_{run_id:04d}_{analysis_id:03d}.txt",
    ]
    qsub_str = " ".join([str(s) for s in qsub_opts])
    batch_cmd = "qsub {} {}".format(qsub_str, cmd)

    print("\n\n", batch_cmd, "\n\n")
    sp.run(batch_cmd, shell=True)


def get_analysis_id(run_ids):

    """
    We want each analysis run simultaneously to have the same analysis number.
    This function goes through and builds the directory structure out and
    checks to see what the next possible analysis index is such that all run_ids
    recieve the same analysis index.

    """

    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")

    analysis_ids = []
    for run_id in run_ids:

        run_id_dir = base_path / Path(f"rid_{run_id:04d}")

        if not run_id_dir.is_dir():
            run_id_dir.mkdir()
            print(f"Created directory: {run_id_dir}")

        analysis_dirs = glob(str(run_id_dir) + "/*/")
        print(list(analysis_dirs))

        test = [str(f.name)[-3:] for f in run_id_dir.iterdir() if f.is_dir()]
        print(test)

        analysis_id = len(analysis_dirs)
        analysis_ids.append(analysis_id)
        print(
            f"\nlist of analysis IDs detected: {analysis_ids}. max = {max(analysis_ids)} "
        )

    return max(analysis_ids)


if __name__ == "__main__":
    main()
