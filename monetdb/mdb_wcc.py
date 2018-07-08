import networkx as nx
import matplotlib
import pandas as pd
from networkx import *
import graphlab as gl
import pandas as pd
import pymonetdb
import util


log = util.setup_log('mdb_wcc')

class MDB_WCC:
    def __init__(self, node_f, edge_f, db, node_tbl, edge_tbl):
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

            cursor = self.connection.cursor()

            # reset all components ids
            cursor.execute('''update %s set cc=node_id;''' %(node_tbl))
        except Exception as e:
            log.error(e)
            return


    def compute_wcc(self):
        steps = 0
        cursor = self.connection.cursor()

        while True:
            try:
                cursor.execute('''
                                update %s set cc=v_tmp.cc from
                                (
                                    select v1.node_id, min(v2.cc) as cc
                                    from %s as v1, %s as e, %s as v2
                                    where v1.node_id = e.dst and v2.node_id = e.src
                                    group by v1.node_id, v1.cc
                                    having min(v2.cc) < v1.cc
                                ) as v_tmp
                                where %s.node_id = v_tmp.node_id and %s.cc <> v_tmp.cc;
                            ''' %(node_tbl, node_tbl, edge_tbl, node_tbl, node_tbl, node_tbl))
                steps += 1

                if cursor.rowcount <= 0:
                    log.info("Finished WCC computation after %d steps." %steps)
                    self.steps = steps
                    break

            except pymonetdb.OperationalError as e:
                log.error(e)
                #connection.rollback()
            except Exception as e:
                log.error(e)


    def get_n_components(self):
        pass

    def test_wcc(self):
        pass
