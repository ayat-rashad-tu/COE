import re
import os
import numpy as np
import pandas as pd
import networkx
import snap
import matplotlib
from matplotlib import pyplot as plt
from scipy.stats import skewnorm
import csv



def save_graph(g, gname, out_path=""):
    # save graph to node and edge files

    #snap.SaveEdgeList(g, "%s/%s_edge.csv" %(out_path, gname))
    edges = pd.DataFrame([(e.GetSrcNId(),e.GetDstNId()) for e in g.Edges()], columns=['src', 'dst'])
    edges2 = edges.copy()
    edges2.columns = ['dst', 'src']
    edges = pd.concat((edges, edges2))
    edges.to_csv("%s/%s_edge.csv" %(out_path, gname), index=False, header=False, sep=' ')

    nodes = pd.DataFrame([n.GetId() for n in g.Nodes()], columns=['node_id'])
    nodes['cc'] = nodes['node_id']
    nodes['dist'] = -1
    nodes.to_csv("%s/%s_node.csv" %(out_path, gname), index=False, header=False)


def get_deg_dist(g):
    # extract vertices degree distribution of graph (g)
    CntV = snap.TIntPrV()
    snap.GetOutDegCnt(g, CntV)

    deg_dist = pd.DataFrame([(p.GetVal1(), p.GetVal2()) for p in CntV], columns=["deg","cnt"])
    deg_dist['type'] = 'out_deg'

    CntV = snap.TIntPrV()
    snap.GetInDegCnt(g, CntV)

    deg_dist2 = pd.DataFrame([(p.GetVal1(), p.GetVal2()) for p in CntV], columns=["deg","cnt"])
    deg_dist2['type'] = 'in_deg'

    all_deg = pd.concat((deg_dist,deg_dist2))

    return all_deg


def get_props(g, gname, out_path="", fast=False, to_file=True):
    # get properties of a graph, e.g. density, connected components, diameter, etc.

    if to_file:
        desc_f = "%s/%s_desc.txt" %(out_path, gname)
    else:
        desc_f = "/dev/stdout"

    snap.PrintInfo(g, "description", desc_f, fast)

    all_deg = get_deg_dist(g)

    if to_file:
        deg_f = "%s/%s_deg_dist.csv" %(out_path,gname)
        all_deg.to_csv(deg_f, index=False)

    #else:
    return all_deg


def plt_deg_dist(deg_dist, max_deg=50, dtype="InDegree"):
    # plot vertices degree distribution

    print "nodes with degree greater than %d:" %max_deg, deg_dist.query('deg>%d'%max_deg).shape[0]
    print "average degree:", (deg_dist.deg * deg_dist.cnt * 1.0/deg_dist.cnt.sum()).sum()

    plt.figure()
    plt.bar(deg_dist.query('deg<=%d'%max_deg).deg,deg_dist.query('deg<=%d'%max_deg).cnt)
    plt.title(dtype)


def extract_subgraph(g, n, skew):
    # extract a subgraph of size (n) from graph (g).
    # choose the subgraph vertices with weights normally distributed with skew.

    nodes = pd.DataFrame([(n.GetId(),n.GetInDeg()) for n in g.Nodes()], columns=['node_id','in_deg'])
    nodes['cc'] = nodes['node_id']
    nodes['dist'] = -1

    skew = skew
    rv = skewnorm(skew, loc=50, scale=10)
    weights = rv.pdf(nodes.in_deg)
    nodes['weight'] = weights
    u_deg = nodes.sort_values('in_deg').drop_duplicates('in_deg')
    print u_deg.shape

    plt.plot(u_deg.in_deg, u_deg.weight, 'k-', lw=1)
    plt.xlim(0,50)
    #plt.scatter(nodes.in_deg, weights)

    thresh = 5.0e-3
    high_prop_nodes = nodes.query("weight>%f"%thresh)
    subg_nodes = np.random.choice(high_prop_nodes.node_id, n, replace=False,
                                  p=high_prop_nodes.weight/high_prop_nodes.weight.sum()).tolist()

    all_neighbors = []

    for n in subg_nodes:
        mx_n = 5
        NodeVec = snap.TIntV()
        snap.GetNodesAtHop(g, n, 1, NodeVec, False)
        neighbors = [neig for neig in NodeVec]
        all_neighbors.extend(neighbors)

    subg_nodes.extend(all_neighbors)

    NIdV = snap.TIntV()
    for n in np.unique(subg_nodes):
        NIdV.Add(n)

    subg = snap.GetSubGraph(g, NIdV)
    print subg.GetNodes()

    return subg


def add_random_edges(subg, n, skew):
    # add a number (n) of random edges to graph (subg)
    # choose the source vertices with weights normally distributed with skew.

    nodes = pd.DataFrame([(n.GetId(),n.GetInDeg()) for n in subg.Nodes()], columns=['node_id','in_deg'])

    rv = skewnorm(skew, loc=0, scale=5)
    weights = rv.pdf(nodes.in_deg)
    nodes['weight'] = weights
    u_deg = nodes.sort_values('in_deg').drop_duplicates('in_deg')

    plt.plot(u_deg.in_deg, u_deg.weight, 'k-', lw=1)
    plt.xlim(0,50)

    low_deg_nodes = np.random.choice(nodes.node_id, 1000, replace=False,
                                  p=nodes.weight/nodes.weight.sum()).tolist()
    new_edges = list(itertools.combinations_with_replacement(low_deg_nodes, 2))

    print subg.GetEdges()

    for ne in new_edges[:n]:
        if ne[0]== ne[1]:
            continue
        subg.AddEdge(ne[0], ne[1])

    print subg.GetEdges()

    return subg
