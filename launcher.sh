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
    echo "Not implemented."
}

analyzer() {
    echo "Not implemented."
}

if [[ $# -lt 1 ]]; then
    echo "Usage: launcher.sh -[i/a/r/o]"
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

# Now, launch the analyzer. We send our input to a named pipe.
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
