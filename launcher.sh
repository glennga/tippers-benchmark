#!/bin/bash
set -e

if [[ $# -ge 2 ]] && [[ $@ == *mysql* ]]; then
    database_opt="mysql"
elif [[ $# -ge 2 ]] && [[ $@ == *postgres* ]]; then
    database_opt="postgres"
else
    echo "Usage: launcher.sh [-r/-x/-n] [mysql/postgres]"
    exit 1
fi

# Initialize the databases.
if [[ $@ == *-n* ]]; then
    mkdir -p results # Assuming that we are starting in the repo directory.
    python3 initializer.py ${database_opt} none 2>/dev/null
    exit 0
fi

# Re-setup the experiment if specified.
if [[ $@ == *-r* ]]; then
    python3 destructor.py ${database_opt}
    python3 initializer.py ${database_opt} none 2>/dev/null
    exit 0
fi

# Start the experiments.
if [[ $@ == *-x* ]]; then
    observer() {
        runner_spawn_date=$(date +"%Y-%m-%d %T.%N")
        echo "[${runner_spawn_date::-3}][launcher.sh] Runner spawned w/ PID $1."

        rm /tmp/observer.fifo 2> /dev/null || true
        mkfifo /tmp/observer.fifo  # We send our input to a named pipe.
        </tmp/observer.fifo tail -c +1 -f | python3 observer.py ${database_opt} false > /dev/null &
        observer_pid=$!

        observer_spawn_date=$(date +"%Y-%m-%d %T.%N")
        echo "[${observer_spawn_date::-3}][launcher.sh] Observer spawned w/ PID ${observer_pid}."
        wait $1  # Wait for the runner to stop before stopping the observer.
        echo "\n" > /tmp/observer.fifo
        echo > /tmp/observer.fifo
        rm /tmp/observer.fifo
        wait ${observer_pid}
    }
    runner() {
        sleep 0.5 # Wait for observer to run first...
        python3 runner.py ${database_opt} $1 $2 $3 $4
    }
    restarter() {
        python3 destructor.py ${database_opt}
        python3 initializer.py ${database_opt} $1
    }

    # Get our experiment parameters.
    IFS=', ' read -r -a testing_concurrency \
        <<< "$(sed -n '/testing-concurrency/p' config/general.json | cut -d '[' -f 2 | cut -d ']' -f 1)"
    IFS=', ' read -r -a testing_workload \
        <<< "$(sed -n '/testing-workload/p' config/general.json | cut -d '[' -f 2 | cut -d ']' -f 1)"
    IFS=', ' read -r -a testing_mpl \
        <<< "$(sed -n '/testing-mpl/p' config/general.json | cut -d '[' -f 2 | cut -d ']' -f 1)"

    for concurrency in "${testing_concurrency[@]}"; do
        concurrency=$(echo ${concurrency%\"})
        concurrency=$(echo ${concurrency#\"})

        for workload in "${testing_workload[@]}"; do
            workload=$(echo ${workload%\"})
            workload=$(echo ${workload#\"})

            for mpl in "${testing_mpl[@]}"; do
                if [[ ${workload} == "i" ]]; then
                    # INSERT-only workloads run at read committed.
                    restarter ${concurrency}
                    runner ${workload} ${concurrency} rc ${mpl} &
                    observer $!

                elif [[ ${workload} == "q" ]]; then
                    # QUERY-only workloads run at read committed. No reset is required.
                    runner ${workload} ${concurrency} rc ${mpl} &
                    observer $!

                else
                    # For COMPLETE workloads, test all isolation levels.
                    restarter ${concurrency}
                    runner ${workload} ${concurrency} ru ${mpl} &
                    observer $! # Read uncommitted.

                    restarter ${concurrency}
                    runner ${workload} ${concurrency} rc ${mpl} &
                    observer $! # Read committed.

                    restarter ${concurrency}
                    runner ${workload} ${concurrency} rr ${mpl} &
                    observer $! # Repeatable reads.

                    restarter ${concurrency}
                    runner ${workload} ${concurrency} s ${mpl} &
                    observer $! # Serializable.
                fi
            done
        done
    done

    echo "Experiments are finished!"
    exit 0
fi
