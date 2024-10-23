#!/bin/sh
#SBATCH -N 1 #Request 1 nodes
#SBATCH -L scratch4@slurmdb
#SBATCH --time=16:00:00
#SBATCH --signal=B:SIGTERM@900
#SBATCH -A y24_ccm
#SBATCH --output=/lustre/scratch5/aschneider/data/2023/logs/slurm_logs/fp3-slurm-%J_%A_%a-%N.out

# Source your environment script
source /users/aschneider/workspaces/CCM_cray/env.sh

##################

TERM_SIGNAL_RECEIVED=0
trap  "TERM_SIGNAL_RECEIVED=1" SIGTERM

# Figure out where this script is
if [ -n "${SLURM_JOB_ID:-}" ] ; then
    SOURCE=$(scontrol show job "$SLURM_JOB_ID" | awk -F= '/Command=/{print $2}' | head -n 1)
else
    SOURCE=${BASH_SOURCE[0]}
fi
while [ -L "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )
    SOURCE=$(readlink "$SOURCE")
    [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPT_DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )
SCRIPT_NAME=$(basename ${SOURCE})
SCRIPT_PATH="${SCRIPT_DIR}/${SCRIPT_NAME}"


#####################################################################
#####################################################################

# Define the job list file and the python script you want to run with
JOBS_LIST=$SCRIPT_DIR/source_jobs.txt
PYTHON_SCRIPT=$SCRIPT_DIR/pulse_finding.py

# Set the number of tasks you want to run simultaneously per node
N_JOBS=10

#####################################################################
#####################################################################

# Start all the worker scripts
PIDS=()
for i in $(seq 1 $N_JOBS)
do
    echo "Starting process $i"
    python $SCRIPT_DIR/slurm_worker.py --jobs-list $JOBS_LIST --python-script $PYTHON_SCRIPT --python-flush-buffer &
    PID=$!
    PIDS+=($PID)
    sleep 5
done

CHECK_INTERVAL=30
NUM_CHECKS_BEFORE_EXIT=5

function num_running {
    N_RUNNING=0
    for PID in ${PIDS[@]}
    do
        ps -p $PID > /dev/null
        STATUS=$?
        if [ $STATUS -eq 0 ]
        then
            N_RUNNING=$((N_RUNNING+1))
        fi
    done
    echo $N_RUNNING
}

# Monitor the running processes and forward any SIGTERM signals received to them
while true
do
    for PID in ${PIDS[@]}
    do
        ps -p $PID > /dev/null
        STATUS=$?
        if [ $STATUS -ne 0 ]
        then
            continue
        fi

        if [ $TERM_SIGNAL_RECEIVED -eq 1 ]
        then
            echo "TERM signal received"
            echo "Killing process $PID"
            kill -s 2 $PID
            break
        fi
    done
    if [ $(num_running) -eq 0 ]
    then
        break
    fi
    sleep $CHECK_INTERVAL
done

count=0


# Once a SIGTERM signal is received and forwarded to the processes, wait for them to finish
while true
do
    for PID in ${PIDS[@]}
    do
        echo "Checking if process $PID is still running"
        ps -p $PID > /dev/null
        STATUS=$?
        if [ $STATUS -ne 0 ]
        then
            echo "Process $PID has finished"
            continue
        else
            echo "Process $PID is still running"
        fi

    done
    if [ $(num_running) -eq 0 ]
    then
        echo "All processes have finished"
        break
    else
        echo "Processes is still running"
        count=$((count+1))
        echo "Count is $count"
    fi

    # check if count is greater than or equal to 5
    if [ $count -ge $NUM_CHECKS_BEFORE_EXIT ]
    then
        echo "Processes still running after $NUM_CHECKS_BEFORE_EXIT checks"
        for PID in ${PIDS[@]}
        do
            echo "Killing process $PID"
            kill -s 2 $PID
        done
        break
    fi
    echo "Waiting $CHECK_INTERVAL seconds"
    sleep $CHECK_INTERVAL
done

echo "Waiting to finish"
wait
echo "Processes have finished"
