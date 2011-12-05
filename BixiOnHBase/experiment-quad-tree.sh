#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)


shortTestByTime="
1hour_10s
6hour_10s
12hour_10s
18hour_10s
24hour_10s"

longTestByTime="
1day_10s
5day_10s
10day_10s
15day_10s
20day_10s
"

shortTestByStation="
1station_12h
5station_12h
10station_12h
15station_12h
20station_12h
"

longTestByStation="
1station_10d
50station_10d
100station_10d
200station_10d
300station_10d
400station_10d
"
location="location"

export shortTestByTime longTestByTime shortTestByStation longTestByStation location

for j in {1..4}; do
echo "********$j Times*********************************************************\n"

for i in $shortTestByTime; do
echo "********Short Analysis**by time******callTimeSlot4StationsScan***$i***********"
${myDir}/call_quad_tree.sh 1 $i
echo "********Short Analysis**by time**Coprocessor****callTimeSlot4Stations***$i***********"
${myDir}/call_quad_tree.sh 3 $i
done

for i in $shortTestByStation;
do
echo "********Short Analysis**by station******callTimeSlot4StationsScan***$i***********"
${myDir}/call_quad_tree.sh 1 $i
echo "********Short Analysis**by station***Coprocessor***callTimeSlot4Stations***$i***********"
${myDir}/call_quad_tree.sh 3 $i
done

for i in $longTestByTime;
do
echo "********Long Analysis**by time******callTimeSlot4StationsScan***$i***********"
${myDir}/call_quad_tree.sh 1 $i
echo "********Long Analysis**by time***Coprocessor***callTimeSlot4Stations***$i***********"
${myDir}/call_quad_tree.sh 3 $i
done

for i in $longTestByStation;
do
echo "********Long Analysis**by time******callTimeSlot4StationsScan***$i***********"
${myDir}/call_quad_tree.sh 1 $i
echo "********Long Analysis**by time**Coprocessor****callTimeSlot4Stations***$i***********"
${myDir}/call_quad_tree.sh 3 $i
done

echo "**************callTimeSlot4PointScan***$location***********"
${myDir}/call_quad_tree.sh 2 $location
echo "***********Coprocessor***callTimeSlot4Point***$location***********"
${myDir}/call_quad_tree.sh 4 $location

done # done with many times

echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
