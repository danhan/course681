#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)


locations="
s2_l1
s2_l2
s2_l3
s2_l4
s2_l5
s2_l6
s2_l7
s2_l8
"
c_locations="
s2_l11
s2_l21
s2_l31
s2_l41
s2_l51
s2_l61
s2_l71
"

export locations c_locations

for j in {1..5}; do
echo "********$j Times*********************************************************\n"
for i in $locations; do
echo "*********schema2**$i***callTimeSlot4PointScan*************"
${myDir}/call_quad_tree.sh 2 $i
done

done # done with many times

for j in {1..5}; do
echo "********$j Times*********************************************************\n"
for i in $c_locations; do
echo "*********schema2**Coprocessor**$i*callTimeSlot4Point************"
${myDir}/call_quad_tree.sh 4 $i
done

done # done with many times



echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
