hadoop namenode -format
#start-dfs.sh
#start-mapred.sh
#$HADOOP_HOME/sbin/start-all.sh

rm /tmp/hadoop-hduser/dfs/data/current/VERSION

$HADOOP_HOME/sbin/start-dfs.sh
#$HADOOP_HOME/sbin/start-mapred.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Change log level
hadoop daemonlog -setlevel localhost:50060 org.apache.hadoop.hdfs.server.TaskTracker DEBUG
hadoop daemonlog -setlevel localhost:50030 org.apache.hadoop.hdfs.server.JobTracker DEBUG
hadoop daemonlog -setlevel localhost:50075 org.apache.hadoop.hdfs.server.datanode DEBUG

#hadoop dfs -ls /user/hduser/input
