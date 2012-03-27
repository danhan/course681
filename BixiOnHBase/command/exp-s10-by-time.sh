#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

shortTestByTime="
1hour_10s
6hour_10s
12hour_10s
24hour_10s
2day_10s
4day_10s
8day_10s
16day_10s
"

export shortTestByTime

for j in {1..5}; do
echo "********$j Times*********************************************************\n"

for i in $shortTestByTime; do
echo "********Short Analysis**by time***schema1**$i*callTimeSlot4StationsScan**************"
${myDir}/call_schema1.sh 1 $i
echo "********Short Analysis**by time***schema2**$i*callTimeSlot4StationsScan**************"
${myDir}/call_quad_tree.sh 1 $i
echo "********Short Analysis**by time**schema1 Coprocessor**$i**callTimeSlot4Stations**************"
${myDir}/call_schema1.sh 3 $i
echo "********Short Analysis**by time**schema2 Coprocessor**$i***callTimeSlot4Stations*************"
${myDir}/call_quad_tree.sh 3 $i
done

done # done with many times

echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
