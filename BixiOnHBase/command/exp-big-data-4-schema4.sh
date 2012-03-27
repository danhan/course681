#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

shortTestByTime="
1hour_200s
6hour_200s
12hour_200s
24hour_200s
2day_200s
4day_200s
8day_200s
16day_200s
"

export shortTestByTime

for j in {1..1}; do
echo "********$j Times*********************************************************\n"

for i in $shortTestByTime; do
echo "********Short Analysis**by time**schema1 Coprocessor**$i**callTimeSlot4Stations**************"
${myDir}/call_schema4.sh 3 $i

done

done # done with many times

echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
