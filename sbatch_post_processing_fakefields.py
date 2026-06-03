#!/usr/bin/env python3
import argparse
import csv
import subprocess as sp
from pathlib import Path
import shlex


def main():
    """
    Slurm submission helper for fake-field-aware 2025 LTF post-processing.

    Stage 0 builds saved_experiments/<experiment_name>_aid_<analysis_id>/root_files.csv.
    Stage 1 reads that root_files.csv and submits one job per pp_file_id, not one job
    per physical file_id. This is the key fix for fake-field scans, because the same
    run_id/file_id can appear once per fake-field analysis_variant.
    Stage 2 merges the per-pp_file_id outputs.
    """

    sp.run(["umask u=rwx,g=rwx,o=rx"], executable="/bin/bash", shell=True)

    par = argparse.ArgumentParser()
    arg = par.add_argument

    arg("-t", "--tlim", nargs=1, type=str, help="set time limit (HH:MM:SS)")
    arg("-rids", "--run_ids", nargs="+", type=int, required=True,
        help="run ids to collect track data for")
    arg("-aid", "--analysis_id", type=int, required=True,
        help="analysis_id to collect track data for")
    arg("-name", "--experiment_name", type=str, required=True,
        help="name used to write the experiment to disk")
    arg("-nft", "--num_files_tracks", type=int, default=-1,
        help="max number of pp_file_id rows for which to save tracks; -1 means all")
    arg("-nfp", "--num_files_points", type=int, default=-1,
        help="max number of pp_file_id rows for which to save track points; -1 means all")
    arg("-stage", "--stage", type=int, required=True,
        help="0: setup, 1: process pp_file_id jobs, 2: merge")
    arg("-ms_standard", "--ms_standard", type=int, required=True,
        help="0: root names to seconds, 1: root names to ms")
    arg("-ffs", "--fake_fields", nargs="+", type=float, default=None,
        help="fake fields to include. Omit for ordinary aid_### post-processing")
    arg("--post_processing_script", type=str,
        default="/data/raid2/eliza4/he6_cres/rocks_analysis_pipeline/run_post_processing_2025LTF.py",
        help="path to fake-field-aware run_post_processing_2025LTF.py on the cluster")
    arg("--container", type=str,
        default="/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif",
        help="apptainer container path")
    arg("--bind", type=str,
        default="/data/raid2/eliza4/he6_cres/",
        help="apptainer bind path")

    args = par.parse_args()
    tlim = "12:00:00" if args.tlim is None else args.tlim[0]

    if args.stage not in [0, 1, 2]:
        raise ValueError("stage must be 0, 1, or 2")

    rids_formatted = " ".join(str(rid) for rid in args.run_ids)
    fake_fields_arg = ""
    if args.fake_fields is not None:
        fake_fields_arg = " -ffs " + " ".join(str(ff) for ff in args.fake_fields)

    if args.stage == 0:
        file_ids = [-1]
        nft = args.num_files_tracks
        nfp = args.num_files_points

    elif args.stage == 1:
        root_files_csv = get_root_files_csv(args.experiment_name, args.analysis_id)
        file_ids = get_pp_file_ids(root_files_csv)

        if args.num_files_tracks >= 0:
            file_ids = [fid for fid in file_ids if fid < args.num_files_tracks]
            nft = args.num_files_tracks
        else:
            nft = max(file_ids) + 1 if file_ids else 0

        if args.num_files_points >= 0:
            nfp = args.num_files_points
        else:
            nfp = max(file_ids) + 1 if file_ids else 0

        print("Submitting {} stage-1 jobs using pp_file_id values from {}".format(
            len(file_ids), root_files_csv
        ))
        print("pp_file_ids:", file_ids)

    else:
        file_ids = [-1]
        root_files_csv = get_root_files_csv(args.experiment_name, args.analysis_id)
        pp_file_ids = get_pp_file_ids(root_files_csv)

        if args.num_files_tracks >= 0:
            nft = args.num_files_tracks
        else:
            nft = max(pp_file_ids) + 1 if pp_file_ids else 0

        if args.num_files_points >= 0:
            nfp = args.num_files_points
        else:
            nfp = max(pp_file_ids) + 1 if pp_file_ids else 0

    for file_id in file_ids:
        post_processing_cmd = build_post_processing_cmd(
            script=args.post_processing_script,
            rids_formatted=rids_formatted,
            aid=args.analysis_id,
            name=args.experiment_name,
            nft=nft,
            nfp=nfp,
            fid=file_id,
            stage=args.stage,
            ms_standard=args.ms_standard,
            fake_fields_arg=fake_fields_arg,
        )
        cmd = wrap_in_apptainer(
            post_processing_cmd,
            container=args.container,
            bind=args.bind,
        )
        print(cmd)
        sbatch_job(args.experiment_name, args.analysis_id, file_id, cmd, tlim)


def get_root_files_csv(experiment_name, analysis_id):
    base_path = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/saved_experiments")
    root_files_csv = base_path / "{}_aid_{}".format(experiment_name, analysis_id) / "root_files.csv"
    if not root_files_csv.is_file():
        raise FileNotFoundError(
            "Missing {}. Run stage 0 first, or check experiment_name/analysis_id.".format(root_files_csv)
        )
    return root_files_csv


def get_pp_file_ids(root_files_csv):
    with root_files_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return []

    if "pp_file_id" in rows[0]:
        ids = sorted({int(float(row["pp_file_id"])) for row in rows if row.get("pp_file_id", "") != ""})
    elif "file_id" in rows[0]:
        print("WARNING: pp_file_id not found. Falling back to physical file_id.")
        ids = sorted({int(float(row["file_id"])) for row in rows if row.get("file_id", "") != ""})
    else:
        raise KeyError("root_files.csv must contain pp_file_id or file_id")

    return ids


def build_post_processing_cmd(
    script,
    rids_formatted,
    aid,
    name,
    nft,
    nfp,
    fid,
    stage,
    ms_standard,
    fake_fields_arg,
):
    return (
        "/opt/python3.7/bin/python3.7 -u "
        "{script} "
        "-rids {rids} -aid {aid} -name {name} "
        "-nft {nft} -nfp {nfp} -fid {fid} -stage {stage} "
        "-ms_standard {ms_standard}"
        "{fake_fields_arg}"
    ).format(
        script=shlex.quote(script),
        rids=rids_formatted,
        aid=aid,
        name=shlex.quote(name),
        nft=nft,
        nfp=nfp,
        fid=fid,
        stage=stage,
        ms_standard=ms_standard,
        fake_fields_arg=fake_fields_arg,
    )


def wrap_in_apptainer(cmd, container, bind):
    inner = "umask 002; source /data/raid2/eliza4/he6_cres/.bashrc; {}".format(cmd)
    return (
        "apptainer exec --bind {bind} {container} /bin/bash -lc {inner}"
    ).format(
        bind=shlex.quote(bind),
        container=shlex.quote(container),
        inner=shlex.quote(inner),
    )


def sbatch_job(experiment_name, analysis_id, file_id, cmd, tlim):
    """Submit an inline command via Slurm's --wrap."""
    log_dir = Path("/data/raid2/eliza4/he6_cres/katydid_analysis/job_logs/post_processing")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "{}_aid_{}_fid_{}.txt".format(experiment_name, analysis_id, file_id)

    sbatch_cmd = [
        "sbatch",
        "--job-name", "{}_a{}_f{}".format(experiment_name, analysis_id, file_id),
        "--time", tlim,
        "--output", str(log_path),
        "--export=ALL",
        "--mail-type=NONE",
        "--wrap", cmd,
    ]

    print("\n\n{}\n\n".format(" ".join(shlex.quote(x) for x in sbatch_cmd)))
    sp.run(sbatch_cmd, check=False)


if __name__ == "__main__":
    main()
