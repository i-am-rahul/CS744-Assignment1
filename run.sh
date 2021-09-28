#!/bin/bash

if [[ $# -eq 0 ]]; then
	echo "./run.sh <master_ip> <jar_path> <run_class> <input_hdfs_location> <output_hdfs_location> <persists> <col_name_to_partition> <number_partition>"
	echo "example: ./run.sh c220g2-010607vm-1.wisc.cloudlab.us pagerank_2.12-0.1.jar org.rakab.SparkPageRank hdfs://10.10.1.1:9000/data/task2/enwiki-pages-articles/ true fromNode -1" 
	exit
fi

MASTER_HOSTNAME=spark://$1:7077
JAR_PATH=$2
RUN_CLASS=$3
NUM_EXEC=2
EXEC_CORES=5
EXEC_MEMORY=30G
DRIVER_MEMORY=30G

shift 3

a1=$1
a2=$2
a3=$3
a4=$4
a5=$5


echo
echo running application now
echo

set -x

time -p $(find . -name spark-submit) --class "${RUN_CLASS}" --master ${MASTER_HOSTNAME} \
	--executor-cores ${EXEC_CORES} \
	--executor-memory ${EXEC_MEMORY} \
	--driver-memory ${DRIVER_MEMORY} \
	--driver-cores 1 \
	${JAR_PATH} $a1 $a2 $a3 $a4 $a5
