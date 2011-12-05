#!/bin/bash

run=2
for i in {1..2};
do
echo $i
done
myDir=$(readlink -f $0 | xargs dirname)

location="location"
echo "**************callTimeSlot4PointScan***$location***********"
${myDir}/call_quad_tree.sh 2 $location
echo "***********Coprocessor***callTimeSlot4Point***$location***********"
${myDir}/call_quad_tree.sh 4 $location
