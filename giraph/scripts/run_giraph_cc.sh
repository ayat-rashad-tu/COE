export algorithm="org.apache.giraph.GiraphRunner org.apache.giraph.examples.ConnectedComponentsComputation"

#export vif="org.apache.giraph.io.formats.IntIntNullTextInputFormat"
export vif="org.apache.giraph.io.formats.LongLongNullTextInputFormat"
#export vif=""

#export eif="org.apache.giraph.io.formats.IntNullTextEdgeInputFormat"
#export eif="org.apache.giraph.io.formats.IntNullReverseTextEdgeInputFormat"
export eif=""

export vof="org.apache.giraph.io.formats.IdWithValueTextOutputFormat"

#export com="org.apache.giraph.combiner.MinimumIntMessageCombiner"
export com=""

export input_dir="/dataset/orkut"
export input="orkut_edge_al.txt"
export output="cc_orkut"
export metrics_dir="cc_orkut_t4"

export n_cores=`nproc`

export jar_loc="/home/hduser/COE/giraph/giraph-examples/target/giraph-algorithms-jar-with-dependencies.jar"

#cp ${input_dir}/${input} /tmp/${input}
#hadoop dfs -rmr /user/hduser/input/${input}
hadoop dfs -copyFromLocal ${input_dir}/${input} /user/hduser/input/${input}
hadoop dfs -rmr /user/hduser/output/${output}
hadoop dfs -rmr /user/hduser/${metrics_dir}

export run_giraph="hadoop jar ${jar_loc} ${algorithm}  -vof ${vof}"

if [ ! -z "${vif}" ]
then
        export run_giraph=$run_giraph" -vif ${vif} -vip /user/hduser/input/${input}"
fi

if [ ! -z "${eif}" ]
then
        export run_giraph=$run_giraph" -eif ${eif} -eip /user/hduser/input/${input}"
fi

if [ ! -z "${com}" ]
then
        export run_giraph=$run_giraph" -c ${com}"
fi

export run_giraph=$run_giraph" -op /user/hduser/output/${output} -w 1 -ca giraph.SplitMasterWorker=false"
export run_giraph=$run_giraph" -ca giraph.metrics.enable=true"
export run_giraph=$run_giraph" -ca giraph.metrics.directory=${metrics_dir}"
export run_giraph=$run_giraph" -ca giraph.numComputeThreads=$n_cores"
export run_giraph=$run_giraph" -ca giraph.numInputThreads=$n_cores"
export run_giraph=$run_giraph" -ca giraph.numOutputThreads=12"
#export run_giraph=$run_giraph" -ca giraph.io.edge.reverse.duplicator=true"

eval $run_giraph


