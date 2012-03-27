#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

shortTestByTime="
1hour_100s
6hour_100s
12hour_100s
24hour_100s
2day_100s
4day_100s
8day_100s
16day_100s
"

export shortTestByTime

for j in {1..4}; do
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
