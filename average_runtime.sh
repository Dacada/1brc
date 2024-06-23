#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <command> [args...]"
    exit 1
fi

command="$@"
total_runs=8
discard_runs=3
measure_runs=$((total_runs - discard_runs))
total_time=0

for i in $(seq 1 $total_runs); do
    real_time=$( ( { /usr/bin/time -f "%e" $command 1>/dev/null; } 2>&1 ) )
    if [ $i -gt $discard_runs ]; then
        echo "$real_time"
        total_time=$(echo "$total_time + $real_time" | bc)
    else
        echo "discard..."
    fi
done

average_time=$(echo "scale=3; $total_time / $measure_runs" | bc)
echo "Average runtime: $average_time seconds"
