#!/bin/bash
# Check hadoop logs if there were any errors.

printf "DATANODE LOGS: \n"
tail -n 50 $HADOOP_HOME/logs/hadoop-hduser-datanode-ip-172-31-54-48.log

printf "\n\n\n"
printf "YARN LOGS..RM: \n"
tail -n 50 $HADOOP_HOME/logs/yarn-hduser-resourcemanager-ip-172-31-54-48.log

printf "\n\n\n"
printf "YARN LOGS..NM: \n"
tail -n 50 $HADOOP_HOME/logs/yarn-hduser-nodemanager-ip-172-31-54-48.log



