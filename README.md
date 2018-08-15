# Graph Processing: Clash Of Engines.

The experiments used two algorithms: Weakly Connected Components (WCC) and Breadth First Search (BFS). 

The repository contents:
* giraph:  
  * giraph-examples: the implementation of these two algorithms in java.
  * scripts:
    * check_dfs.sh:	check the logs of Hadoop for any errors
    * check_giraph.sh:	check the logs of Hadoop jobs for errors
    * run_giraph_bfs.sh: run BFS algorithm
    * run_giraph_bfs_ooc.sh: run BFS algorithm using Out-Of-Core configuration to allow giraph to process a graph exceeding memory size. that was only used for Friendster dataset. the other datasets fit in memory.
    * run_giraph_cc.sh: run WCC algorithm
    * run_giraph_cc_ooc.sh:	run WCC algorithm with out-of-core configuration. 
    * start_dfs.sh: start hadoop and the other components.
    * stop_dfs.sh: stop hadoop.

* monetdb: the implementation of the algorithms in python to work with monetdb. The scripts directory contains scripts for loading data into monetdb and for montitoring monetdb using psrecord. 
  * scripts:
    * monitor_mdb.sh: monitor resources usage of monetdb using psrecord.
    * prepare_data.sh: load data from files into monetdb database.
  * graph.py: functions for reading, writing, and processing graph data.
  * mdb_bfs.py: BFS implementation in python.
  * mdb_wcc.py: WCC implementation
  * tests.py: simple tests to check the algorithms are working as expected.
  * util.py: utility functions.
  
* benchmark: the metrics extracted from the log files of hadoop, monetdb query_history, and psrecord files.
  * parse_log_final.ipynb: python notebook used for parsing the metrics files and creating the plots.
