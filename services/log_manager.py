from multiprocessing import Process
import sys
import signal
import logging

from utils import log_config


class LogManager(Process):
    def __init__(self, log_queue):
        Process.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.queue = log_queue

    def on_terminate(self, num, frame):
        self.logger.error(log_config.log_error(module_name=__name__, function_name='on_terminate',
                                               error='Log Manager Terminated'))
        sys.exit()

    def run(self):
        self.logger.info(log_config.log_success(module_name=__name__, function_name='run',
                                                msg='Log Manager Started'))
        signal.signal(signal.SIGTERM, self.on_terminate)
        while True:
            log_item = self.queue.get()
            if not log_item:
                self.logger.info(log_config.log_success(module_name=__name__, function_name='run',
                                                        msg='Log Manager Shut Down'))
                break
            getattr(self.logger, log_item['type'])(log_item['message'])