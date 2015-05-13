from multiprocessing import Process, Queue
import logging

from milter_config import postgresql_creds
from utils import log_config
from services.postgre_manager import PostgreManager
from services.log_manager import LogManager

log_config.init()
log_config.set_level(is_dev=True)

postgre_queue = Queue(maxsize=0)
log_queue = Queue(maxsize=0)


class TestClass(Process):

    def run(self):
        try:
            postgre_queue.put({
                'command': {
                    'type': 'select',
                    'table_name': 'attachments',
                    'columns': ['file_name']
                },
                'kwargs': {
                    'return_dicts': True
                }
            })
            log_queue.put({
                'type': 'info',
                'message': 'put in queue.'
            })
        except Exception as e:
            log_queue.put({
                'type': 'error',
                'message': str(e)
            })
        finally:
            log_queue.put({
                'type': 'info',
                'message': 'test class closing.'
            })


try:
    pgr_mgr = PostgreManager(postgresql_creds, postgre_queue, log_queue)
    pgr_mgr.start()
    log_mgr = LogManager(log_queue)
    log_mgr.start()
    test_class = TestClass()
    test_class.start()
    test_class_two = TestClass()
    test_class_two.start()
    test_class_three = TestClass()
    test_class_three.start()
    test_class.join()
    test_class_two.join()
    test_class_three.join()
    postgre_queue.put(None)
    pgr_mgr.join()
    log_queue.put(None)
    log_mgr.join()
    logging.debug('exit')
except Exception as err:
    logging.debug(err)