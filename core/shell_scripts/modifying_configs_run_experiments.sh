#!/bin/bash

if [ $# -eq 2 ];
then
#    instances=$2
    query=$1
    memory=$2
    sed -i "33s/[0-9]\+/${memory}/g" ./tpcds-executors-spark.conf

    for inc in {2..10..2}
    do
      sed -i "30s/[0-9]\+/${inc}/g" ./tpcds-executors-spark.conf

      for i in {1..3..1}

      do
         echo $i
          ./Tpcds_${query}_500G.sh
      done
    done


else
	echo "
	You should define two paramter:
	1.queryName(q1, q2 or q23a .etc);
	2.executor memory in MB
	usuage example:  bash modifying_configs_run_experiments.sh q1 900
	"
fi

#sed -i '1s/[#*]/fff/gp' file    --表示针对文件第1行，将其中的#号或是*号替换为fff


#sed -i "30s/[1-9]\+/10/g" ./tpcds-executors-spark.conf