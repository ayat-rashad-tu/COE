import networkx as nx
import matplotlib
import pandas as pd
from networkx import *
import graphlab as gl
import pandas as pd
import pymonetdb
import util


log = util.setup_log('mdb_bfs')

class MDB_BFS:
    def __init__(self, node_f, edge_f, db, node_tbl, edge_tbl, src_node=0):
        self.edge_f = edge_f
        self.node_f = node_f
        self.db = db
        self.edge_tbl = edge_tbl
        self.node_tbl = node_tbl
        self.steps = -1

        try:
            self.connection = pymonetdb.connect(username="monetdb", password="monetdb",
                                       hostname="localhost", database=db,
                                       autocommit=True
                                      )

        except Exception as e:
            log.error(e)


    def reset(self):
        try:
            cursor = self.connection.cursor()

            # reset all distances
            cursor.execute('''update %s set dist=-1;''' %(self.node_tbl))
            # set the distance of start node to 0
            cursor.execute('''update %s set dist=0 where node_id=%d;''' %(self.node_tbl, self.src_node))
            return 1
        except Exception as e:
            log.error(e)
            return 0


    def compute_bfs(self):
        steps = 0

        if not self.reset():
            return

        cursor = self.connection.cursor()

        while True:
            try:
                cursor.execute('''
                                update %s set dist=v_tmp.dist from
                                (
                                    select v1.node_id, v2.dist+1 as d
                                    from %s as v1, %s as e, %s as v2
                                    where v1.node_id = e.dst and v2.node_id = e.src
                                    and v1.dist = -1 and v2.dist <> -1
                                ) as v_tmp
                                where %s.node_id = v_tmp.node_id;
                            ''' %(self.node_tbl, self.node_tbl, self.edge_tbl, self.node_tbl, self.node_tbl))

                steps += 1

                if cursor.rowcount <= 0:
                    log.info("Finished BFS computation after %d steps." %steps)
                    self.steps = steps
                    break

            except pymonetdb.OperationalError as e:
                log.error(e)
                #connection.rollback()
            except Exception as e:
                log.error(e)


    def get_distances(self):
        pass

    def test_bfs(self):
        pass
