#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

short="
sd1
sd2
sd3
sd4
sd5
sd6
sd7
sd8
sd9
"

long="
d1
d2
d3
d4
d5
d6
d7
d8
d9
"

export short, long

for j in {1..1}; do
echo "********$j Times*********************************************************\n"

for i in $short; do
echo "**********$i****************"
${myDir}/call_location_s2.sh 1 $i
done

for ii in $short; do
echo "*********Coprocessor for $ii**********"
${myDir}/call_location_s2.sh 2 $ii
done

for dd in $long; do
echo "**********Coprocessor for *$dd*****************"
${myDir}/call_location_s2.sh 2 $dd
done


done # done with many times

echo "========================================================"
echo "================================Experiment End ========="
echo "========================================================"
