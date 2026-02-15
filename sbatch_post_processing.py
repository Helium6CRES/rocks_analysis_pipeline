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
    arg(
        "-rids",
        "--run_ids",
        nargs="+",
        type=int,
        help="list of runids to collect track data for.",
    )
    arg(
        "-aid",
        "--analysis_id",
        type=int,
        help="analysis_id to collect track data for.",
    )

    arg(
        "-name",
        "--experiment_name",
        type=str,
        help="name used to write the experiment to disk.",
    )

    arg(
        "-nft",
        "--num_files_tracks",
        type=int,
        help="number of files for which to save track data per run_id.",
    )

    arg(
        "-nfp",
        "--num_files_points",
        type=int,
        help="number of files for which to save track points data per run_id.",
    )

    # Pipeline options (keep parity with existing run_post_processing.py flags)
    arg(
        "-stage",
        "--stage",
        type=int,
        help="""0: set-up. The root file df will be made and the results directory will be build.
                1: processing. The tracks and events will be extracted from root files and written 
                    to disk in the results directory. 
                2: clean-up. The many different csvs worth of tracks and events will be combined into 
                    single files. 
            """,
    )
    arg(
        "-ms_standard",
        "--ms_standard",
        type=int,
        help="""0: Root file names only to second. %Y-%m-%d-%H-%M-%S
                1: Root file names to ms. "%Y-%m-%d-%H-%M-%S-%f"
            """,
    )

    args = par.parse_args()

    tlim = "12:00:00" if args.tlim is None else args.tlim[0]

    # Command to run inside the container (mirrors sbatch_katydid.py pattern)
    # Note: the literal \n is required in the bash -c $'...' string for multi-line commands.
    apptainer_prefix = (
        "\"apptainer exec --bind /data/raid2/eliza4/he6_cres/ "
        "/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif "
        "/bin/bash -c $'umask 002; source /data/raid2/eliza4/he6_cres/.bashrc {} "
    ).format(r"\n")

    # Base command to your pipeline (keep flags aligned with run_post_processing.py expectations)
    base_post_processing_cmd = (
        "/opt/python3.7/bin/python3.7 -u "
        "/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_post_processing_2025LTF.py "
        "-rids {rids} -aid {aid} -name \"{name}\" "
        "-nft {nft} -nfp {nfp} -fid {fid} -stage {stage} "
        "-ms_standard {ms_standard}"
    )

    rids_formatted = " ".join(str(rid) for rid in args.run_ids)

    if args.stage == 0:
        file_id = -1
        post_processing_cmd = base_post_processing_cmd.format(
            rids=rids_formatted,
            aid=args.analysis_id,
            name=args.experiment_name,
            nft=args.num_files_tracks,
            nfp=args.num_files_points,
            fid=file_id,
            stage=args.stage,
            ms_standard=args.ms_standard
        )
        cmd = apptainer_prefix + f"{post_processing_cmd}'\""
        print(cmd)

        sbatch_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)

    if args.stage == 1:

        files_to_process = args.num_files_tracks
        print(f"Submitting {files_to_process} jobs.")

        for file_id in range(files_to_process):

            post_processing_cmd = base_post_processing_cmd.format(
                rids=rids_formatted,
                aid=args.analysis_id,
                name=args.experiment_name,
                nft=args.num_files_tracks,
                nfp=args.num_files_points,
                fid=file_id,
                stage=args.stage,
                ms_standard=args.ms_standard
            )
            cmd = apptainer_prefix + f"{post_processing_cmd}'\""
            print(cmd)

            sbatch_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)

    if args.stage == 2:
        file_id = -1
        post_processing_cmd = base_post_processing_cmd.format(
            rids=rids_formatted,
            aid=args.analysis_id,
            name=args.experiment_name,
            nft=args.num_files_tracks,
            nfp=args.num_files_points,
            fid=file_id,
            stage=args.stage,
            ms_standard=args.ms_standard
        )
        cmd = apptainer_prefix + f"{post_processing_cmd}'\""
        print(cmd)

        sbatch_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)

    # Done at the beginning and end of qsub main.
    #set_permissions()


def sbatch_job(experiment_name: str, analysis_id: int, file_id: int, cmd: str, tlim: str):
    """
    Submit an inline command via Slurm's --wrap.
    """
    log_dir = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/post_processing")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{experiment_name}_aid_{analysis_id}.txt"

    sbatch_opts = [
        "--job-name", f"{experiment_name}_a{analysis_id}_f{file_id}",
        "--time", tlim,
        "--output", str(log_path),
        "--export=ALL",
        "--mail-type=NONE",
    ]

    sbatch_str = " ".join(sbatch_opts)
    batch_cmd = f"sbatch {sbatch_str} --wrap={cmd}"

    print("\n\n", batch_cmd, "\n\n")
    sp.run(batch_cmd, shell=True)

    return None


if __name__ == "__main__":
    main()
