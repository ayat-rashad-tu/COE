package org.apache.giraph.examples;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException; 


@Algorithm(name = "Breadth First Search Structural oriented", description = "Uses Breadth First Search from a source vertex to calculate depths")
public class SimpleBFSStructureComputation
		extends
		BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
	/**
	 * Define a maximum number of supersteps
	 */
	public final int MAX_SUPERSTEPS = 9999;

	/**
	 * Indicates the first vertex to be computed in superstep 0.
	 */
	public static final LongConfOption START_ID = new LongConfOption(
			"SimpleBFSComputation.START_ID", 8,
			"Is the first vertex to be computed");

	/** Class logger */
	private static final Logger LOG = Logger
			.getLogger(SimpleBFSStructureComputation.class);

	/**
	 * Is this vertex the start vertex?
	 * 
	 * @param vertex
	 * @return true if analysed node is the start vertex
	 */
	private boolean isStart(Vertex<IntWritable, ?, ?> vertex) {
		return vertex.getId().get() == START_ID.get(getConf());
	}

	/**
	 * Send messages to all the connected vertices. The content of the messages
	 * is not important, since just the event of receiving a message removes the
	 * vertex from the inactive status.
	 * 
	 * @param vertex
	 */
	public void BFSMessages(
			Vertex<IntWritable, IntWritable, NullWritable> vertex) {
		for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
			sendMessage(edge.getTargetVertexId(), new IntWritable(1));
		}
	}

	@Override
	public void compute(
			Vertex<IntWritable, IntWritable, NullWritable> vertex,
			Iterable<IntWritable> messages) throws IOException {

		// Forces convergence in maximum superstep
		if (!(getSuperstep() == MAX_SUPERSTEPS)) {
			// Only start vertex should work in the first superstep
			// All the other should vote to halt and wait for
			// messages.
			if (getSuperstep() == 0) {
				if (isStart(vertex)) {
					vertex.setValue(new IntWritable((int)getSuperstep()));
					BFSMessages(vertex);
					if (LOG.isInfoEnabled()) {
						LOG.info("[Start Vertex] Vertex ID: " + vertex.getId());
					}
				} else { // Initialise with infinite depth other vertex
					vertex.setValue(new IntWritable(Integer.MAX_VALUE));
				}
			}

			// if it is not the first Superstep (Superstep 0) :
			// Check vertex ID

			else {
				// It is the first time that this vertex is being computed
				if (vertex.getValue().get() == Integer.MAX_VALUE) {
					// The depth has the same value that the superstep
					vertex.setValue(new IntWritable((int)getSuperstep()));
					// Continue on the structure
					BFSMessages(vertex);
				}
				// Else this vertex was already analysed in a previous
				// iteration.
			}
			vertex.voteToHalt();
		}
	}
}

