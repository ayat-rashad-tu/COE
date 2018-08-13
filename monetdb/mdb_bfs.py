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
        self.metrics = None
        self.n_updates = []

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


    def start_tracking(self, dataset, out_path):
        # enable the query log in MonetDB, to log the metrics
        cursor.execute('call sys.querylog_enable();')
        cursor.execute('call sys.querylog_empty();')

        pid = subprocess.check_output(['pgrep', 'mserver5']).strip()
        memory_metrics_f = "%s/bfs_%s_mem.txt" %(out_path, dataset)
        # use psrecords to watch MonetDB process to get the RAM, Virtual memory metrics
        subprocess.Popen(['psrecord', pid,'--log',
                          memory_metrics_f,'--include-children', '--duration', '1800',
                          '--interval', '1'])


    def get_metrics(self, dataset, out_path):
        # read the metrics from the database and files
        metrics = pd.DataFrame()

        try:
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
            metrics.to_csv("%s/bfs_%s.csv" %(out_path, dataset), index=False, header=True)
            self.metrics = metrics

        except Exception as e:
            log.error(e)


    def compute_bfs(self):
        steps = 0
        self.n_updates = []

        if not self.reset():
            return

        self.start_tracking(dataset, out_path)

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
                self.n_updates.append(cursor.rowcount)

                if cursor.rowcount <= 0:
                    log.info("Finished BFS computation after %d steps." %steps)
                    self.steps = steps
                    break

            except pymonetdb.OperationalError as e:
                log.error(e)
            except Exception as e:
                log.error(e)



    def get_distances(self):
        pass

    def test_bfs(self):
        pass
