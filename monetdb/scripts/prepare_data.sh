#!/bin/bash
# Load the graph data from the files to the database 

export node_f="../../data/nodes_s.csv"
export edge_f="../../data/edges_s.csv"
export node_t=node_c
export edge_t=edge_c

cp $node_f $edge_f /tmp

mclient -u monetdb -d graph_data -s "truncate ${edge_t};" 
mclient -u monetdb -d graph_data -s "truncate ${node_t};"

mclient -u monetdb -d graph_data -s "COPY INTO ${edge_t} FROM '/tmp/${edge_f}' delimiters ',';"
mclient -u monetdb -d graph_data -s "COPY INTO ${node_t} FROM '/tmp/${node_f}' delimiters ',';"
