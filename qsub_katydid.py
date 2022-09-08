#!/usr/bin/env python3
import subprocess as sp
import argparse
from typing import List

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
    arg, st = par.add_argument, 'store_true'
    # arg('--job', nargs=1, type=str, help='command to execute, usually in quotes')
    arg("-t", "--tlim", nargs=1, type=str, help='set time limit (HH:MM:SS)')
    arg('-rids', '--runids', nargs='+', type=int, help='run ids to analyze')
    arg('-b','--base_config', type=str, help='base .yaml katydid config file to be run on run_id, should exist in base config directory.')
    arg('-fn', '--file_num', default=-1, type=int, help='Number of files in run id to analyze (<= number of files in run_id)')

    args = par.parse_args()

    tlim = '12:00:00' if args.tlim is None else args.tlim[0]

    # cmd = args.job[0]

    #Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.
    con = "\"singularity exec --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif /bin/bash -c $\'source /data/eliza4/he6_cres/.bashrc {} ".format(r"\n")
    # cmd = con + f"{args.job[0]}\'\""

#    if args.job:
    for run_id in args.runids:
        default_katydid_sub = 'python3 /data/eliza4/he6_cres/python_dev/katydid_on_rocks_dev_qsub.py -id {} -b \"{}\" -fn {}'.format(run_id, args.base_config, args.file_num)
        cmd = con + f"{default_katydid_sub}\'\""
        qsub_job(run_id, cmd, tlim)


def qsub_job(run_id, cmd, tlim):
    """
    ./qsub.py --job 'arbitrary command' [options]

    NOTE: redirecting the log file to a sub-folder is a little tricky
    https://stackoverflow.com/questions/15089315/redirect-output-to-different-files-for-sun-grid-engine-array-jobs-sge
    """
    qsub_opts = [
        "-S /bin/bash", # use bash
        "-cwd", # run from current working directory
        "-m n", # don't send email notifications
        "-w e", # verify syntax
        "-V", # inherit environment variables
        "-N cres_analysis", # job name
        f"-l h_rt={tlim}", # time limit
        "-q all.q", # queue name (cenpa only uses one queue)
        "-j yes", # join stderr and stdout
        "-b y", # Look for series of bytes.
        "-o /data/eliza4/he6_cres/job_logs/output_{}.txt".format(run_id),
        # "-t {}-{}".format(1,len(run_ids)) # job array mode.  example: 128 jobs w/ label $SGE_TASK_ID
    ]
    qsub_str = ' '.join([str(s) for s in qsub_opts])
    batch_cmd = "qsub {} {}".format(qsub_str, cmd)

    # Or maybe we should just do a for loop to submit the jobs.

    # add singularity command

    # Notes from Sam:

    # $SGE_TASK_ID is an env variable that we can use to


    print("\n\n", batch_cmd,"\n\n")
    sp.run(batch_cmd, shell = True)


if __name__=="__main__":
    main()

