#!/usr/bin/env python3
import subprocess as sp
import argparse
from pathlib import Path

def main():
    """
    DOCUMENT
    """

    umask = sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    # Parse command line arguments.
    par = argparse.ArgumentParser()
    arg = par.add_argument

    # Workload description
    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg( "-r", "--run_name", type=str, help="runname to collect track data for.",) #XXX: fix me to allow for multiple?
    arg( "-aid", "--analysis_id", type=int, help="analysis_id to collect track data for.",)
    arg( "-e", "--experiment_name", type=str, help="name used to write the experiment to disk.",)
    arg("-d", "--dry_run", action='store_true', help='Skip slurm submission')

    args = par.parse_args()

    tlim = "12:00:00" if args.tlim is None else args.tlim[0]

    # Command to run inside the container (mirrors sbatch_katydid.py pattern)
    # Note: the literal \n is required in the bash -c $'...' string for multi-line commands.
    apptainer_prefix = (
        "\"apptainer exec --bind /data/raid2/eliza4/he6_cres/ "
        "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
        "/bin/bash -c $'umask 002; source /data/raid2/eliza4/he6_cres/.bashrc {} "
    ).format(r"\n")

    # Base command to your pipeline (keep flags aligned with run_ssa_post_processing.py expectations)
    base_post_processing_cmd = (
        "/opt/python3.7/bin/python3.7 -u "
        "/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_ssa_post_processing.py "
        "-r {runs} -a {aid} -e \"{exps}\" "
    )
    post_processing_cmd = base_post_processing_cmd.format(
        runs=args.run_name,
        aid=args.analysis_id,
        exps=args.experiment_name,
    )

    cmd = apptainer_prefix + f"{post_processing_cmd}'\""
    print(cmd)

    sbatch_job(args.experiment_name, args.run_name, args.analysis_id, cmd, tlim, args.dry_run)

    # Done at the beginning and end of qsub main.
    #set_permissions()


def sbatch_job(experiment_name: str, run_name: str, analysis_id: int, cmd: str, tlim: str, dry_run: bool):
    """
    Submit an inline command via Slurm's --wrap.
    """
    machine_path = Path("/data/raid2/eliza4/he6_cres")
    #machine_path = Path("/Users/buzinsky/fake_wulf/")
    log_dir = machine_path / Path("spec_sims_analysis/job_logs/post_processing")
    #log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{experiment_name}_a_{analysis_id}.txt"

    sbatch_opts = [
        "--job-name", f"{experiment_name}_a{analysis_id}",
        "--time", tlim,
        "--output", str(log_path),
        "--export=ALL",
        "--mail-type=NONE",
    ]

    sbatch_str = " ".join(sbatch_opts)
    batch_cmd = f"sbatch {sbatch_str} --wrap={cmd}"

    print("\n\n", batch_cmd, "\n\n")
    if not dry_run:
        sp.run(batch_cmd, shell=True)

    return None


if __name__ == "__main__":
    main()
