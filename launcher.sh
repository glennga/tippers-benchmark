#!/bin/bash

initializer() {
    mysql create.sql
}

analyzer() {
    python3 analyzer.py \
        postgres \
        false \
        --frequency 0.1
}

runner() {
    python3 runner.py
}

if [[ $# -lt 1 ]]; then
    echo "Usage: launcher.sh -[i/a/r]"
    exit 1
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

# Finally, launch the analyzer. We send our input to a named pipe.
if [[ $@ == *-a* ]] && [[ $@ == *-r* ]]; then
    mkfifo /tmp/analyzer.fifo
    </tmp/analyzer.fifo tail -c +1 -f | analyzer > /dev/null &
    analyzer_pid=$!

    wait ${runner_pid}
    echo "\n" > /tmp/analyzer.fifo
    echo > /tmp/analyzer.fifo
    rm /tmp/analyzer.fifo
    wait ${analyzer_pid}
fi


