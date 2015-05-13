from multiprocessing.managers import BaseManager
from multiprocessing import Process, Queue
import sys
import signal
import logging
import time

from services import postgre
from services.postgre import Postgre
from milter_config import postgresql_creds

logging.basicConfig(level=logging.DEBUG)

postgre_queue = Queue(maxsize=2)


class ConnManager(Process):

    def __init__(self):
        Process.__init__(self)
        self.postgre = Postgre(postgresql_creds, is_pool=True, minconn=2, maxconn=2)

    def on_terminate(self, num, frame):
        logging.debug('terminated')
        self.postgre.close()
        sys.exit()

    def run(self):
        signal.signal(signal.SIGTERM, self.on_terminate)
        try:
            while True:
                returned_conn = postgre_queue.get()
                if not returned_conn:
                    break

        finally:
            logging.debug('closed')
            self.postgre.close()


class TestClass(object):

    def __init__(self):
        asdf = postgre_queue.get()
        # pgr = postgre_queue.get()
        # with pgr.get_connection() as conn:
        #     t = postgre.transaction(conn,
        #                             {
        #                                 'type': 'select',
        #                                 'table_name': 'attachments',
        #                                 'columns': ['file_name']
        #                             }, return_dicts=True)
        # print pgr.pool._used
        # with pgr.get_connection() as conn:
        #     t = postgre.transaction(conn,
        #                             {
        #                                 'type': 'select',
        #                                 'table_name': 'attachments',
        #                                 'columns': ['file_name']
        #                             }, return_dicts=True)
        # print pgr.pool._used
        # with pgr.get_connection() as conn:
        #     t = postgre.transaction(conn,
        #                             {
        #                                 'type': 'select',
        #                                 'table_name': 'attachments',
        #                                 'columns': ['file_name']
        #                             }, return_dicts=True)
        # print pgr.pool._used

try:
    a = ConnManager()
    a.start()
    postgre_queue.put(None)
    a.join()
    logging.debug('exit')
except:
    pass