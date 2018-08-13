import networkx as nx
import matplotlib
import pandas as pd
from networkx import *
import graphlab as gl
import pandas as pd
import pymonetdb
from subprocess import call
import subprocess
import time

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
        self.metrics = None

        try:
            self.connection = pymonetdb.connect(username="monetdb", password="monetdb",
                                       hostname="localhost", database=db,
                                       autocommit=True
                                      )

        except Exception as e:
            log.error(e)
            return


    def reset(self):
        try:
            cursor = self.connection.cursor()

            # reset all components ids
            cursor.execute('''update %s set cc=node_id;''' %(node_tbl))

            return 1
        except Exception as e:
            log.error(e)
            return 0


    def start_tracking(self, dataset, out_path):
        # enable the query log in MonetDB, to log the metrics
        cursor.execute('call sys.querylog_enable();')
        cursor.execute('call sys.querylog_empty();')

        pid = subprocess.check_output(['pgrep', 'mserver5']).strip()
        memory_metrics_f = "%s/cc_%s_mem.txt" %(out_path, dataset)
        # use psrecords to watch MonetDB process to get the RAM, Virtual memory metrics
        subprocess.Popen(['psrecord', pid,'--log',
                          memory_metrics_f,'--include-children', '--duration', '1800',
                          '--interval', '1'])


    def get_metrics(self, dataset, out_path):
        # read the metrics from the database and files
        metrics = pd.DataFrame()

        try:
            memory_metrics_f = "%s/cc_%s_mem.txt" %(out_path, dataset)
            mem_usage = pd.read_csv(memory_metrics_f, sep='\s+',
                                names=['time','cpu', 'RAM','virtual'],
                               skiprows=[0])

            mem_usage = mem_usage.query('cpu>10.0')

            with self.connection.cursor() as cursor:
                cursor.execute('select start,stop,query,run,cpu,io,ship,tuples from sys.querylog_history;')
                metrics = pd.DataFrame.from_records(cursor.fetchall(),
                                                    columns='start,stop,query,run,cpu,io,ship,tuples'.split(','))
            metrics['RAM'] = mem_usage['RAM'].mean()
            metrics['Virtual'] = mem_usage['virtual'].mean()
            metrics.to_csv("%s/cc_%s.csv" %(out_path, dataset), index=False, header=True)
            self.metrics = metrics

        except Exception as e:
            log.error(e)


    def compute_wcc(self, dataset, out_path):
        '''
        out_path: the directory to write the metrics files
        dataset: the name of the dataset
        '''
        steps = 0
        cursor = self.connection.cursor()

        if not self.reset():
            log.error('Could not reset the data tables.')
            return

        self.start_tracking(dataset, out_path)

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
                                where %s.node_id = v_tmp.node_id;
                            ''' %(self.node_tbl, self.node_tbl, self.edge_tbl, self.node_tbl, self.node_tbl))
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

        self.get_metrics(dataset, out_path)


    def get_n_components(self):
        with self.connection.cursor() as cursor:
            cursor.execute('''select distict cc from %s; ''' %self.node_tbl)
            n_comp = cursor.rowcount()
            return n_comp

    def test_wcc(self):
        pass
