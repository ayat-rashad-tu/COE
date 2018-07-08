import networkx as nx
import matplotlib
import pandas as pd
from networkx import *


def create_star(n, edge_f, node_f):
    g = nx.star_graph(n)
    return g


def create_2cc(n, edge_f, node_f):
    G = nx.star_graph(n)
    new_nodes = [n+15 for n in G.nodes]
    new_edges = [(n1+15, n2+15) for n1,n2 in G.edges]
    [G.add_node(n) for n in new_nodes]
    [G.add_edge(src,dst) for src,dst in new_edges]

    return G


def create_pl(n, edge_f, node_f):
    G = nx.powerlaw_cluster_graph(n, 5, .1)


def describe(g):
    print("radius: %d" % radius(g))
    print("diameter: %d" % diameter(g))
    #print("eccentricity: %s" % eccentricity(g))
    #print("center: %s" % center(g))
    #print("periphery: %s" % periphery(g))
    print("density: %s" % density(g))


def save_graph(g, node_f='nodes.csv', edge_f='edges.csv'):
    nodes = pd.DataFrame({'node_id':[n for n in G.nodes]})
    nodes['cc'] = nodes['node_id']
    nodes['dist'] = -1
    nodes.to_csv(node_f, header=False, index=False)

    edges = pd.DataFrame.from_records([e for e in g.edges], columns=['src','dst'])
    #symmetrize
    edges = pd.concat((edges, pd.DataFrame.from_records([e for e in G.edges], columns=['dst', 'src'])))
    edges.to_csv(edge_f, header=False, index=False)


def show(g):
    pass


def load_gl_graph(node_f='nodes.csv', edge_f='edges.csv'):
    nodes = pd.read_csv(node_f)
    edges = pd.read_csv(edge_f, names=['src','dst'])

    g = gl.SGraph()
    g = g.add_edges(edges, src_field='src', dst_field='dst')
    g = g.add_vertices(nodes)

    return g


def setup_log(log_name):
    pass
