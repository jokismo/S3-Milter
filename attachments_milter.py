from multiprocessing import Queue
import Milter

from utils import log_config
from services.s3_milter import S3Milter
from services.log_manager import LogManager
from services.postgre_manager import PostgreManager
from milter_config import postgresql_creds
from milter_config import milter_params

log_config.init()
log_config.set_level(is_dev=True)
log_queue = Queue(maxsize=0)
postgre_queue = Queue(maxsize=0)


class QueuedS3Milter(S3Milter):

    def __init__(self):
        S3Milter.__init__(self)
        self.log_queue = log_queue
        self.postgre_queue = postgre_queue


def execute():
    log_manager = LogManager(log_queue)
    log_manager.start()
    postgre_manager = PostgreManager(postgresql_creds, postgre_queue, log_queue)
    postgre_manager.start()
    Milter.factory = QueuedS3Milter
    Milter.set_flags(Milter.MODBODY)
    log_queue.put({
        'type': 'info',
        'message': log_config.log_success(module_name=__name__, function_name='execute',
                                          msg='S3 Milter service launched.')
    })
    Milter.runmilter('S3Milter', milter_params['socket_name'], milter_params['timeout'])
    log_queue.put({
        'type': 'info',
        'message': log_config.log_success(module_name=__name__, function_name='execute',
                                          msg='S3 Milter service shutdown.')
    })
    postgre_queue.put(None)
    postgre_manager.join()
    log_queue.put(None)
    log_manager.join()


if __name__ == "__main__":
    execute()