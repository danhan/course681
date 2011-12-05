#!/bin/bash

# PROGNAME=$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)
#pushd .


#cd ${myDir}/../

#source ./bin/envrc

USAGE="USAGE: run_bixi.sh 1 94 01_10_2010__00"

if [ -z "$1" ]; then
	echo "$USAGE"
	exit -1
fi

if [ ! -f "${JAVA_HOME}/bin/java" ]; then
	echo "JAVA_HOME not found."
	exit -1
fi

if [ ! -f "${HBASE_HOME}/hbase-0.93-SNAPSHOT.jar" ]; then
	echo "HBASE_HOME not found, hbase-0.20.6 is needed."
	exit -1
fi

if [ ! -f "${HADOOP_HOME}/hadoop-0.20.2-core.jar" ]; then
	echo "HADOOP_HOME not found, hadoop-0.20.2 is needed."
	exit -1
fi

COMMONLIB=${HBASE_HOME}/lib/log4j-1.2.16.jar:\
${HBASE_HOME}/lib/commons-logging-1.1.1.jar:\
${HBASE_HOME}/lib/commons-cli-1.2.jar:\
${HBASE_HOME}/lib/zookeeper-3.3.3.jar:


MYLIB=${PWD}/bin/bixi.jar

HBASELIB=${HBASE_HOME}/hbase-0.93-SNAPSHOT.jar
HBASECONF=${HBASE_HOME}/conf

HADOOPLIB=${HADOOP_HOME}/hadoop-0.20.2-core.jar

HADOOPCONF=${HADOOP_HOME}/conf

#echo ${MYLIB}

#echo ${COMMONLIB}:${HBASELIB}:${HBASECONF}:${HADOOPLIB}:${HADOOPCONF}:${MYLIB}:${MYCONF}
${JAVA_HOME}/bin/java -Xmx1500m -classpath ${COMMONLIB}:${HBASELIB}:${HBASECONF}:${HADOOPLIB}:${HADOOPCONF}:${MYLIB}:${MYCONF} experiment.TestCase4QuadTree $*

