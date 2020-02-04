#!/bin/bash

initializer() {
    mkdir -p results # Assuming that we are starting in the repo directory.

    python3 initializer.py \
        postgres

    python3 initializer.py \
        mysql
}

observer() {
    python3 observer.py \
        postgres \
        false \
        --frequency 0.1
}

runner() {
    sleep 0.5 # Wait for analyzer to run first...

    echo "Not implemented."
}

destructor() {
    python3 destructor.py \
        postgres

    python3 destructor.py \
        mysql
}

if [[ $# -lt 1 ]]; then
    echo "Usage: launcher.sh -[i/r/o/d]"
    exit 1
fi

# If given, verify that the user wants to delete all tables in both MySQL and Postgres before proceeding.
if [[ $@ == *-d* ]]; then
    echo "You have chosen to delete all experiment tables in MySQL and Postgres."
    printf "Please enter [y/n] to confirm: "
    while read options; do
        case ${options} in
            y) destructor; break ;;
            n) exit 1; break;;
            *) printf "Invalid input. Please enter [y/n] to confirm: ";;
        esac
    done
fi

# Perform the initialization if specified.
if [[ $@ == *-i* ]]; then
    initializer
fi

# Next, start the experiments.
if [[ $@ == *-r* ]]; then
    runner &
    runner_pid=$!
fi

# Last, launch the observer. We send our input to a named pipe.
if [[ $@ == *-o* ]] && [[ $@ == *-r* ]]; then
    mkfifo /tmp/observer.fifo
    </tmp/observer.fifo tail -c +1 -f | observer > /dev/null &
    observer_pid=$!

    wait ${runner_pid}
    echo "\n" > /tmp/observer.fifo
    echo > /tmp/observer.fifo
    rm /tmp/observer.fifo
    wait ${observer_pid}
fi
