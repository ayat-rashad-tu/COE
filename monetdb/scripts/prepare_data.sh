#!/bin/bash
# Load the graph data from the files to the database 

#export data_dir="/home/hduser/COE/data"
export data_dir="/dataset/orkut"
export node_f="orkut_node.csv"
export edge_f="orkut_edge.txt"
export node_t=node_c
export edge_t=edge_c

#cp "${data_dir}/$node_f" "${data_dir}/$edge_f" /tmp

mclient -u monetdb -d g_data -s "truncate ${edge_t};" 
mclient -u monetdb -d g_data -s "truncate ${node_t};"

mclient -u monetdb -d g_data -s "COPY INTO ${edge_t} FROM '$data_dir/${edge_f}' delimiters ' ';"
mclient -u monetdb -d g_data -s "COPY INTO ${node_t} FROM '$data_dir/${node_f}' delimiters ',';"

# Load Friendster data: ignore some lines with errors.
#mclient -u monetdb -d g_data -s "COPY 3529999500 RECORDS INTO ${edge_t} FROM '/dataset/friendster/${edge_f}' delimiters ' ';"
#mclient -u monetdb -d g_data -s "COPY INTO ${node_t} FROM '/dataset/friendster/${node_f}' delimiters ',';"
