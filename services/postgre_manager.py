from multiprocessing import Process
import sys
import signal

from services import postgre
from utils import log_config


class PostgreManager(Process):

    def __init__(self, creds, postgre_queue, log_queue):
        Process.__init__(self)
        self.log_queue = log_queue
        self.queue = postgre_queue
        try:
            self.postgre = postgre.Postgre(creds)
        except Exception as e:
            self.log('error', log_config.log_error(module_name=__name__, function_name='__init__',
                                                   error=str(e)))


    def on_terminate(self, num, frame):
        self.log('error', log_config.log_error(module_name=__name__, function_name='on_terminate',
                                               error='Postgre Manager Terminated'))
        try:
            self.postgre.close()
        except Exception as e:
            self.log('error', log_config.log_error(module_name=__name__, function_name='on_terminate',
                                                   error=str(e)))
        finally:
            sys.exit()

    def run(self):
        self.log('info', log_config.log_success(module_name=__name__, function_name='__init__',
                                                msg='Postgre Manager Started'))
        signal.signal(signal.SIGTERM, self.on_terminate)
        try:
            while True:
                postgre_item = self.queue.get()
                if not postgre_item:
                    break
                self.execute_transaction(postgre_item)
        finally:
            try:
                self.postgre.close()
            except Exception as e:
                self.log('error', log_config.log_error(module_name=__name__, function_name='run',
                                                       error=str(e)))
            self.log('info', log_config.log_success(module_name=__name__, function_name='run',
                                                    msg='Postgre Manager Shut Down'))

    def execute_transaction(self, postgre_item):
        self.log('debug', log_config.log_status(module_name=__name__, function_name='execute_transaction',
                                                msg='Postgre Item Received'))
        try:
            with self.postgre.get_connection() as conn:
                if postgre_item.get('kwargs') is not None:
                    postgre.transaction(conn, postgre_item['command'], **postgre_item['kwargs'])
                else:
                    postgre.transaction(conn, postgre_item['command'])
                conn.commit()
                self.log('info', log_config.log_success(module_name=__name__, function_name='execute_transaction',
                                                        msg='Postgre Item Received'))
        except Exception as e:
            self.log('error', log_config.log_error(module_name=__name__, function_name='execute_transaction',
                                                   error=str(e)))

    def log(self, log_type, message):
        try:
            self.log_queue.put({
                'type': log_type,
                'message': message
            })
        except Exception:
            pass