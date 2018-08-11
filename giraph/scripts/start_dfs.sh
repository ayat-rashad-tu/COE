#hadoop namenode -format
#hadoop dfs -mkdir -p /user/hduser/input

rm /tmp/hadoop-hduser/dfs/data/current/VERSION

$HADOOP_HOME/sbin/start-dfs.sh
#$HADOOP_HOME/sbin/start-mapred.sh
$HADOOP_HOME/sbin/start-yarn.sh
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

hdfs dfsadmin -safemode leave

# Change log level
hadoop daemonlog -setlevel localhost:50060 org.apache.hadoop.hdfs.server.TaskTracker DEBUG
hadoop daemonlog -setlevel localhost:50030 org.apache.hadoop.hdfs.server.JobTracker DEBUG
hadoop daemonlog -setlevel localhost:50075 org.apache.hadoop.hdfs.server.datanode DEBUG


