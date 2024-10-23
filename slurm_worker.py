#!/usr/bin/env python3
import numpy as np
import argparse
import uuid
import json
import slurm_manager
import subprocess
import sys
import signal
import time

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs-list", required=True, type=str, help="file that contains the list of jobs to process")
    parser.add_argument("--python-script", required=True, type=str, help="Path to the python script that you want to run")
    # Pass either --python-flush-buffer or --no-python-flush-buffer
    parser.add_argument("--python-flush-buffer", action=argparse.BooleanOptionalAction, type=bool, default=True, help="Pass '-u' to the python interpreter to get printouts more immediately")
    args = parser.parse_args()

    slurm_manager = slurm_manager.SlurmManager(args.jobs_list, processing_timeout_hours=17)

    while True:
        job = slurm_manager.acquire_next_job()
        if job is None:
            break
        job_args = job.args
        job_name = job.name
        job_log = job.log
        job_error = job.error
        job_output = job.output

        ### extract parameters for this job
        argument_list = []
        if type(job_args) is dict:
            for key, value in job_args.items():
                argument_list.append(f"--{key}")
                argument_list.append(f"{value}")
        else:
            for key, value in job_args:
                argument_list.append(f"--{key}")
                argument_list.append(f"{value}")

        random_name = str(uuid.uuid4())
        if job_output is None:
            job_output = f"./{random_name}.out"
        if job_error is None:
            job_error = f"./{random_name}.err"
        fout = open(job_output, "w")
        ferr = open(job_error, "w")

        return_code = 0
        try:
            ### call script to run simulation in the background
            executable = sys.executable
            full_argument_list = [executable]
            if args.python_flush_buffer:
                full_argument_list.append("-u")
            full_argument_list.append(args.python_script)
            full_argument_list.extend(argument_list)
            print(" ".join(full_argument_list))
            p = subprocess.Popen(full_argument_list, stdout=fout, stderr=ferr)
            p.wait()
            return_code = p.returncode
        except KeyboardInterrupt as e:
            p.send_signal(signal.SIGINT)
            p.wait()
            return_code = p.returncode
        fout.close()
        ferr.close()
        if (return_code == 0):
            print(f"Finished processing job_name: {job_name}")
            print(f"Marking current job as processed: {job_name}")
            slurm_manager.mark_processed(job_name)
        elif (return_code == 2):
            print("Caught KeyboardInterrupt")
            print(f"Marking current job as unprocessed: {job_name}")
            slurm_manager.mark_unprocessed(job_name)
            break
        else:
            print(f"Caught exception while processing job_name: {job_name}")
            print(f"Marking current job as failed: {job_name}")
            # Cat the output and error files
            with open(job_output, "r") as f:
                print(f.read())
            with open(job_error, "r") as f:
                print(f.read())
            slurm_manager.mark_failure(job_name)
            time.sleep(60)

