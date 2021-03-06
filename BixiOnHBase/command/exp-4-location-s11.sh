#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

distances="
d1
d2
d3
d4
d5
d6
d7
"

export distances

for j in {1..1}; do
echo "********$j Times*********************************************************\n"

for i in $distances; do
echo "********Short Analysis**by time**schema1 Coprocessor**$i**callTimeSlot4Stations**************"
${myDir}/call_location_s11.sh 1 $i

done

done # done with many times

echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
