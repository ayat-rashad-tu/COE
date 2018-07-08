import networkx as nx
import matplotlib
import pandas as pd
from networkx import *
import graphlab as gl
import pandas as pd
import pymonetdb

import mdb_bfs
import mdb_wcc
import util


log = util.setup_log('mdb_test')

def test_mdb_bfs():
    node_f = 'nodes.csv'
    edge_f = 'edges.csv'

    # test with one connected graph with equal distances
    g = util.create_star(10)
    util.save_graph(g, node_f, edge_f)
    util.load_mdb_graph(node_f, edge_f)

    bfs = mdb_bfs.MDB_BFS(node_f, edge_f, db='graph_data', node_tbl='node', edge_tbl='edge', src_node=0)
    bfs.compute_bfs()
    distances = bfs.get_distances()
    # check all distances equal 1
    assert list(distances.dist.unique()) == [1]

    # test with disconnedted graph
    g = util.create_2cc(10)
    util.save_graph(g, node_f, edge_f)
    util.load_mdb_graph(node_f, edge_f)

    bfs = mdb_bfs.MDB_BFS(node_f, edge_f, db='graph_data', node_tbl='node', edge_tbl='edge', src_node=0)
    bfs.compute_bfs()
    distances = bfs.get_distances()
    # check all there are unreachable nodes with distance -1
    assert sorted(list(distances.dist.unique())) == [-1, 1]
    # check individual values of nodes distances

    # test with different distances
    g = util.create_cycle(10)
    util.save_graph(g, node_f, edge_f)
    util.load_mdb_graph(node_f, edge_f)

    bfs = mdb_bfs.MDB_BFS(node_f, edge_f, db='graph_data', node_tbl='node', edge_tbl='edge', src_node=0)
    bfs.compute_bfs()
    distances = bfs.get_distances()
    # check all there are 5 different distances
    assert sorted(list(distances.dist.unique())) == [1, 2, 3, 4, 5]
    # check individual values of nodes distances



def test_mdb_wcc():
    node_f = 'nodes.csv'
    edge_f = 'edges.csv'

    # test with one connected graph with equal distances
    g = util.create_star(10)
    util.save_graph(g, node_f, edge_f)
    util.load_mdb_graph(node_f, edge_f)

    wcc = mdb_wcc.MDB_WCC(node_f, edge_f, db='graph_data', node_tbl='node', edge_tbl='edge')
    wcc.compute_wcc()
    num_components = wcc.get_n_components()
    # check there's one connected component
    assert num_components == 1

    # test with disconnedted graph with 2 connected components
    g = util.create_2cc(10)
    util.save_graph(g, node_f, edge_f)
    util.load_mdb_graph(node_f, edge_f)

    wcc = mdb_wcc.MDB_WCC(node_f, edge_f, db='graph_data', node_tbl='node', edge_tbl='edge')
    wcc.compute_wcc()
    num_components = wcc.get_n_components()
    # check there are two connected components
    assert num_components == 2
