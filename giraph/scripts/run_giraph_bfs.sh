export algorithm="org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimpleBFSStructureComputation"

export vif="org.apache.giraph.io.formats.IntIntNullTextInputFormat"
export eif=""
export vof="org.apache.giraph.io.formats.IdWithValueTextOutputFormat"

#export com="org.apache.giraph.combiner.MinimumIntMessageCombiner"
export com=""

export input="cc_graph.txt"
export output="bfs"

export jar_loc="/home/hduser/COE/giraph/giraph-examples/target/giraph-algorithms-jar-with-dependencies.jar"

#cp ~/${input} /tmp/${input}
hadoop dfs -rmr /user/hduser/input/${input}
hadoop dfs -copyFromLocal /tmp/${input} /user/hduser/input/${input}
hadoop dfs -rmr /user/hduser/output/${output}

export run_giraph="hadoop jar ${jar_loc} ${algorithm} -vif ${vif} -vip /user/hduser/input/${input} -vof ${vof}"

if [ ! -z "${com}" ]
then
	export run_giraph=$run_giraph" -c ${com}"
fi
export run_giraph=$run_giraph" -op /user/hduser/output/${output} -w 1 -ca giraph.SplitMasterWorker=false"

eval $run_giraph


