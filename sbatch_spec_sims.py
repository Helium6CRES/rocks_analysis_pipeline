#!/usr/bin/env python3
import subprocess as sp
import argparse
from typing import List
from pathlib import Path
####################################

def main():
    """
    Slurm submission script for spec-sims Monte Carlo run
    """
    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-t", "--tlim", nargs=1, type=str, default="48:00:00", help="set time limit (HH:MM:SS)")
    arg("-r", "--run_name", type=str, help="run name")
    arg("-nid", "--noise_run_id", type=int, help="run_id to use for noise floor")
    arg("-y", "--yaml_config", type=str, help="base .yaml spec-sims config file to be run")
    arg("-j", "--json_config", type=str, help="base .json spec-sims config file to be run")
    arg("-n", "--num_subruns", type=int,default=1, help="number of sub-runs. Each subrun is the same except for the seed")
    arg("-s0", "--initial_seed", type=int, default=0, help="seed for subrun_id = 0")
    arg("-d", "--dry_run", action='store_true', help='Skip slurm submission')

    args = par.parse_args()

    # Command to run inside the container
    # Note: the \n must be a literal thing not a \n in the python string itself. Be careful with this.

    apptainer_prefix = (
        "\"apptainer exec --bind /data/raid2/eliza4/he6_cres/ "
        "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
        "/bin/bash -c $'umask 002; source /data/raid2/eliza4/he6_cres/.bashrc {} "
    ).format(r"\n")


    #args.num_subruns long list of seeds starting at initial_seed
    rand_seeds = list(range(args.initial_seed, args.initial_seed + args.num_subruns))

    for subrun_id in range(args.num_subruns):
        default_spec_sims_sub = (
                f"/opt/python3.7/bin/python3.7 -u /data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_spec_sims.py "
            f"-r {args.run_name} -nid {args.noise_run_id} -y \"{args.yaml_config}\" -j \"{args.json_config}\" -sr {subrun_id} -s {rand_seeds[subrun_id]} "
        )
        cmd = apptainer_prefix + f"{default_spec_sims_sub}'\""

        sbatch_job(args.run_name, subrun_id, cmd, args.tlim, args.dry_run)


def sbatch_job(run_name, subrun_id, cmd, tlim, dry_run):
    """
    Replaces SGE qsub with Slurm sbatch.
    Uses --wrap for inline command submission.
    """
    log_path = f"/data/raid2/eliza4/he6_cres/simulation/sim_logs/spec_sims/{run_name}_{subrun_id:03d}.txt"

    sbatch_opts = [
        "--job-name", f"{run_name}_s{subrun_id}",
        "--time", tlim,
        "--output", log_path,
        "--export=ALL",
        "--mail-type=NONE",
    ]

    sbatch_str = " ".join(sbatch_opts)
    batch_cmd = f"sbatch {sbatch_str} --wrap={cmd}"

    print("\n\n", batch_cmd, "\n\n")
    if not dry_run:
        sp.run(batch_cmd, shell=True)

if __name__ == "__main__":
    main()
