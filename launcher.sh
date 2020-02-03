#!/bin/bash

initializer() {
    echo "Not implemented."
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

analyzer() {
    echo "Not implemented."
}

deleter() {
    echo "Not implemented."
}

if [[ $# -lt 1 ]]; then
    echo "Usage: launcher.sh -[i/a/r/o/d]"
    exit 1
fi

# If given, verify that the user wants to delete all tables in both MySQL and Postgres before proceeding.
if [[ $@ == *-d* ]]; then
    echo "You have chosen to delete all experiment tables in MySQL and Postgres."
    printf "Please enter [y/n] to confirm: "
    while read options; do
        case ${options} in
            y) deleter; break ;;
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

# Now, launch the observer. We send our input to a named pipe.
if [[ $@ == *-o* ]] && [[ $@ == *-r* ]]; then
    mkfifo /tmp/analyzer.fifo
    </tmp/analyzer.fifo tail -c +1 -f | analyzer > /dev/null &
    analyzer_pid=$!

    wait ${runner_pid}
    echo "\n" > /tmp/analyzer.fifo
    echo > /tmp/analyzer.fifo
    rm /tmp/analyzer.fifo
    wait ${analyzer_pid}
fi

# If desired, run the analyzer.
if [[ $@ == *-a* ]]; then
    analyzer
fi

