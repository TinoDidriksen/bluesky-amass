#!/bin/bash

while true
do
    find . -type f -name 'stream*log' | sort -r | sed 1,10d | xargs -r rm -fv
    DATE=`date -u '+%Y%m%d-%H%M%S'`
    time ionice -c3 nice -n20 ./stream.py 2>&1 | tee -a stream-${DATE}.log
    echo "Died, restarting..."
done
