#!/usr/bin/env python3
import subprocess as sp
import argparse
from typing import List
from pathlib import Path
from glob import glob


def main():
    """
    helper script to run arbitrary jobs from command line on the rocks cluster.

    check running jobs: qstat -u wisecg

    Notes on SGE, qsub, qstat, etc:
    -- https://cenpa.npl.washington.edu/display/CENPA/qsub
    -- https://www.uibk.ac.at/zid/systeme/hpc-systeme/common/tutorials/sge-howto.html

    Example usage:
    $ python3 qsub.py --job [-t] 'python3 my_amazing_script.py'
    $ ./qsub.py -c --job 'python3 /home/wisecg/he6_files.py -c 01'
    """
    par = argparse.ArgumentParser()
    arg, st = par.add_argument, "store_true"
    # arg('--job', nargs=1, type=str, help='command to execute, usually in quotes')
    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--runids", nargs="+", type=int, help="run ids to analyze")
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

    args = par.parse_args()

    tlim = "12:00:00" if args.tlim is None else args.tlim[0]

    # Get the analysis index to use for the list of jobs.
    analysis_index = get_analysis_index(args.runids)
    print(f"analysis_index: {analysis_index}")

    # Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.
    con = "\"singularity exec --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif /bin/bash -c $'source /data/eliza4/he6_cres/.bashrc {} ".format(
        r"\n"
    )

    for run_id in args.runids:
        default_katydid_sub = 'python3 /data/eliza4/he6_cres/rocks_analysis_pipeline/katydid_on_rocks_dev_qsub.py -id {} -ai {} -b "{}" -fn {} '.format(
            run_id, analysis_index, args.base_config, args.file_num
        )
        cmd = con + f"{default_katydid_sub}'\""
        qsub_job(run_id, analysis_index, cmd, tlim)


def qsub_job(run_id, analysis_index, cmd, tlim):
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
        f"-N run_id_{run_id}",  # job name
        f"-l h_rt={tlim}",  # time limit
        "-q all.q",  # queue name (cenpa only uses one queue)
        "-j yes",  # join stderr and stdout
        "-b y",  # Look for series of bytes.
        f"-o /data/eliza4/he6_cres/job_logs/rid_{run_id:04d}_ai_{analysis_index:03d}.txt",
        # "-t {}-{}".format(1,len(run_ids)) # job array mode.  example: 128 jobs w/ label $SGE_TASK_ID
    ]
    qsub_str = " ".join([str(s) for s in qsub_opts])
    batch_cmd = "qsub {} {}".format(qsub_str, cmd)

    print("\n\n", batch_cmd, "\n\n")
    sp.run(batch_cmd, shell=True)


def get_analysis_index(run_ids):

    """
    We want each analysis run simultaneously to have the same analysis number.
    This function goes through and builds the directory structure out and
    checks to see what the next possible analysis index is such that all run_ids
    recieve the same analysis index.

    """

    base_path = Path("/data/eliza4/he6_cres/katydid_analysis/root_files")

    analysis_indices = []
    for run_id in run_ids:

        run_id_dir = base_path / Path(f"rid_{run_id:04d}")

        if not run_id_dir.is_dir():
            run_id_dir.mkdir()
            print(f"Created directory: {run_id_dir}")

        analysis_dirs = glob(str(run_id_dir) + "/*/")
        analysis_index = len(analysis_dirs)
        analysis_indices.append(analysis_index)

    return max(analysis_indices)


if __name__ == "__main__":
    main()
