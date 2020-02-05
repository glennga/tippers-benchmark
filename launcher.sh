#!/bin/bash
set -e

if [[ $# -ge 2 ]] && [[ $@ == *mysql* ]]; then
    database_opt="mysql"
elif [[ $# -ge 2 ]] && [[ $@ == *postgres* ]]; then
    database_opt="postgres"
else
    echo "Usage: launcher.sh [-r/-x] [mysql/postgres]"
    exit 1
fi

# Re-setup the experiment if specified.
if [[ $@ == *-r* ]]; then
    mkdir -p results # Assuming that we are starting in the repo directory.
    python3 destructor.py ${database_opt}
    python3 initializer.py ${database_opt} 2>/dev/null
    exit 0
fi

# Start the experiments.
if [[ $@ == *-x* ]]; then
    observer() {
        rm /tmp/observer.fifo 2> /dev/null || true
        mkfifo /tmp/observer.fifo  # We send our input to a named pipe.
        </tmp/observer.fifo tail -c +1 -f | python3 observer.py ${database_opt} false > /dev/null &
        observer_pid=$!

        wait $1  # Wait for the runner to stop before stopping the observer.
        echo "\n" > /tmp/observer.fifo
        echo > /tmp/observer.fifo
        rm /tmp/observer.fifo
        wait ${observer_pid}
    }
    runner() {
        sleep 0.5 # Wait for observer to run first...
        python3 runner.py ${database_opt} $1 $2 $3
    }

    for concurrency in low high; do
        for experiment in t q w; do
            for mpl in 1 2 3 4 5 6; do
                runner ${experiment} ${concurrency} ${mpl} &
                observer $!
            done
        done
    done

    echo "Experiments are finished!"
    exit 0
fi
