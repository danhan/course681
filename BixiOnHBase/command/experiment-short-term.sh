#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

shortTestByTime="
1hour_10s
2hour_10s
4hour_10s
6hour_10s
8hour_10s
10hour_10s
12hour_10s
14hour_10s
16hour_10s
18hour_10s
20hour_10s
22hour_10s
24hour_10s
1day_1s
2day_1s
5day_1s
10day_1s
15day_1s
20day_1s
"

shortTestByStation="
1station_12h
5station_12h
10station_12h
15station_12h
20station_12h
30station_12h
40station_12h
50station_12h
60station_12h
70station_12h
80station_12h
90station_12h
100station_12h
200station_12h
300station_12h
400station_12h
"

export shortTestByTime shortTestByStation

for j in {1..2}; do
echo "********$j Times*********************************************************\n"

for i in $shortTestByTime; do
echo "********Short Analysis**by time***schema1***callTimeSlot4StationsScan***$i***********"
${myDir}/call_schema1.sh 1 $i
echo "********Short Analysis**by time**schema1 Coprocessor****callTimeSlot4Stations***$i***********"
${myDir}/call_schema1.sh 3 $i
echo "********Short Analysis**by time***schema2***callTimeSlot4StationsScan***$i***********"
${myDir}/call_quad_tree.sh 1 $i
echo "********Short Analysis**by time**schema2 Coprocessor****callTimeSlot4Stations***$i***********"
${myDir}/call_quad_tree.sh 3 $i
done

:<<BLOCK
for i in $shortTestByStation; do
echo "********Short Analysis**by station***schema1***callTimeSlot4StationsScan***$i***********"
${myDir}/call_schema1.sh 1 $i
echo "********Short Analysis**by station***schema1 Coprocessor***callTimeSlot4Stations***$i***********"
${myDir}/call_schema1.sh 3 $i
echo "********Short Analysis**by station***schema2***callTimeSlot4StationsScan***$i***********"
${myDir}/call_quad_tree.sh 1 $i
echo "********Short Analysis**by station**schema2*Coprocessor***callTimeSlot4Stations***$i***********"
${myDir}/call_quad_tree.sh 3 $i
done
BLOCK


done # done with many times

echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
