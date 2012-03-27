#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)


locations="
l1
l2
l3
l4
l5
l6
l7
l8
"

export locations

for j in {1..5}; do
echo "********$j Times*********************************************************\n"
for i in $locations; do
echo "*********schema1**$i***callTimeSlot4PointScan*************"
${myDir}/call_schema1.sh 2 $i
echo "*********schema1**Coprocessor**$i*callTimeSlot4Point************"
${myDir}/call_schema1.sh 4 $i
done

done # done with many times


echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
