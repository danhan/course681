#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)
pushd .

echo "test getAvailBikes\n "
${myDir}/run_bixi_client.sh 1 1 01_10_2010__00

echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "test getAvgUsageForAHr, a date as 01_10_2010__00"
echo "give list of ids with # as delimitor "
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
${myDir}/run_bixi_client.sh 2 1#2#3#4#5#6 01_10_2010__00

echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "test AverageAvailBikesWithScan, 12, sDate<10_10_2010__12>,"
echo " eDate<10_10_2010__15>, scan-batch size<60>, list ofids<12#123#>,"
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
${myDir}/run_bixi_client.sh 12 01_10_2010__00 01_10_2010__00 60 1#2#3#4#5#6 


echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "test getAvailableBikesFromAPoint, and latitude, longitude"
echo "radius and date: 45.508183, -73.554094, 3, 01_10_2010__01 "
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
${myDir}/run_bixi_client.sh 13 45.508183 -73.554094 3 01_10_2010__00

echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "test getAvailableBikesFromAPointWithScan, and latitude, longitude"
echo "radius and date: 45.508183, -73.554094, 3, 01_10_2010__01 "
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
${myDir}/run_bixi_client.sh 3 45.508183 -73.554094 3 01_10_2010__00


