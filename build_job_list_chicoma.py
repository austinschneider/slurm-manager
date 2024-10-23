import slurm_manager
import glob
import os
import collections
import tqdm

jobs_fname = "./source_jobs.txt"

if os.path.exists(jobs_fname):
    raise ValueError(f"File {jobs_fname} already exists")

def run_file_from_fname(fname):
    basename = os.path.basename(fname)

    if basename.endswith(".i3.zst"):
        n = len(".i3.zst")
        basename = basename[:-n]
    segments = basename.split("_")
    run_segment = segments[-2]
    file_segment = segments[-1]
    run = int(run_segment.strip("run"))
    file = int(file_segment.strip("file"))
    return (run, file)

source_runs = {
    #"blank": [
    #    ["2024-03-04", 13999, 14001, 0, 60, "beam-dir"],
    #    ["2024-03-05", 14002, 14003, 0, 60, "mid-dir"],
    #    ["2024-03-05", 14004, 14006, 0, 60, "zero-dir"],
    #],
    "sodium": [
        ["2024-02-29", 13977, 13979, "row1 == 47", 60, "beam-dir"],
        ["2024-02-29", 13980, 13981, "row1 == 47", 60, "mid-dir"],
        ["2024-03-01", 13982, 13983, "row1 == 46", 60, "zero-dir"],
        ["2024-03-01", 13984, 13986, 0, 60, "zero-dir"],
        ["2024-03-02", 13987, 13988, 0, 60, "mid-dir"],
        ["2024-03-02", 13989, 13991, 0, 60, "beam-dir"],
        ["2024-03-03", 13992, 13993, "row5 + 1.8\"", 60, "beam-dir"],
        ["2024-03-03", 13994, 13996, "row5 + 1.8\" == -46cm", 60, "mid-dir"],
        ["2024-03-03", 13997, 13998, "row5 + 1.8\"", 60, "zero-dir"],
    ],
    "cobalt": [
        ["2024-03-08", 14017, 14020, 0, 60, "zero-dir"],
        ["2024-03-09", 14021, 14024, 0, 60, "beam-dir"],
        ["2024-03-10", 14025, 14028, 0, 60, "mid-dir"],
    ],
}

manager = slurm_manager.SlurmManager(jobs_fname)

input_dir = "/lustre/scratch5/aschneider/data/2023/separate_daqs/"
output_dir = "/lustre/scratch5/aschneider/data/2023/pulses/"

geometry_dir = "/lustre/scratch5/aschneider/data/2023/geometry_files/"

log_dir = "/lustre/scratch5/aschneider/data/2023/logs/"

runs = []
for l in source_runs.values():
    for r in l:
        runs.extend(range(r[1], r[2] + 1))

jobs = []

for run in runs:
    run_log_dir = os.path.join(log_dir, f"run{run:06d}")
    os.makedirs(run_log_dir, exist_ok=True)
    m_input_files = os.path.join(input_dir, "millstester", f"run{run:06d}", "*.i3.zst")
    w_input_files = os.path.join(input_dir, "willstester", f"run{run:06d}", "*.i3.zst")
    run_prefix = f"run{run:06d}"
    log_file = os.path.join(
        run_log_dir,
        f"{run_prefix}.log",
    )
    out_file = os.path.join(
        run_log_dir,
        f"{run_prefix}.log",
    )
    err_file = os.path.join(
        run_log_dir,
        f"{run_prefix}.log",
    )
    jobs.append(
        {
            "name": f"source_run{run:06d}",
            "args": [
                ("input-files", m_input_files),
                ("input-files", w_input_files),
            ],
            "log": log_file,
            "output": out_file,
            "error": err_file,
        }
    )

manager.add_jobs(jobs)
