#stop-mapred.sh
#stop-dfs.sh
$HADOOP_HOME/sbin/stop-mapred.sh
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver

jps
