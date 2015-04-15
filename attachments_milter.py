import log_config

import Milter
from Milter.utils import parse_addr
import mime

import logging
from multiprocessing import Process as Thread, Queue
import StringIO
import time
import rfc822
import email

log_config.init(is_dev=True)
logger = logging.getLogger(__name__)
log_queue = Queue(maxsize=0)


def log_all_threads():
    while True:
        log_item = log_queue.get()
        if not log_item:
            break
        if log_item['type'] == 'error':
            logger.error(log_item, exc_info=True)
        else:
            logger.info(log_item)


class S3Milter(Milter.Base):

    def __init__(self):  # Each connection calls new S3Milter
        self.id = Milter.uniqueID()  # Integer incremented with each call.

    @Milter.noreply
    def envfrom(self, mailfrom, *str):
        self.fp = StringIO.StringIO()
        self.canon_from = '@'.join(parse_addr(mailfrom))
        self.fp.write('From {} {}\n'.format(self.canon_from, time.ctime()))
        return Milter.CONTINUE

    @Milter.noreply
    def header(self, name, hval):
        self.fp.write('{}: {}\n'.format(name, hval))  # add header to buffer
        return Milter.CONTINUE

    @Milter.noreply
    def eoh(self):
        self.fp.write('\n')	 # terminate headers
        return Milter.CONTINUE

    @Milter.noreply
    def body(self, chunk):
        self.fp.write(chunk)
        return Milter.CONTINUE

    def eom(self):
        self.fp.seek(0)
        msg = email.message_from_file(self.fp)
        return Milter.REJECT

    def close(self):
        # always called, even when abort is called.  Clean up
        # any external resources here.
        return Milter.CONTINUE

    def abort(self):
        # client disconnected prematurely
        return Milter.CONTINUE

    def log(self, **log_params):
        log_queue.put(log_params)


def execute():
    logger_thread = Thread(target=log_all_threads)
    logger_thread.start()
    socket_name = "/etc/s3milter/sock"
    timeout = 600
    Milter.factory = S3Milter
    flags = Milter.MODBODY
    Milter.set_flags(flags)
    logger.info('S3 Milter service launched.')
    Milter.runmilter('S3Milter', socket_name, timeout)
    log_queue.put(None)
    logger_thread.join()
    logger.info('S3 Milter service shutdown.')

if __name__ == "__main__":
    execute()