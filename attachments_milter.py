import log_config

import Milter
from Milter.utils import parse_addr
import mime

import logging
from multiprocessing import Process as Thread, Queue
import StringIO
import time
import email
import tempfile

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
        self.fp = StringIO.StringIO()

    # @Milter.noreply
    # def envfrom(self, mailfrom, *str):
    #     canon_from = '@'.join(parse_addr(mailfrom))
    #     self.fp.write('From {} {}\n'.format(canon_from, time.ctime()))
    #     return Milter.CONTINUE
    #
    # @Milter.noreply
    # def header(self, name, hval):
    #     self.fp.write('{}: {}\n'.format(name, hval))  # add header to buffer
    #     return Milter.CONTINUE
    #
    # @Milter.noreply
    # def eoh(self):
    #     self.fp.write('\n')	 # terminate headers
    #     return Milter.CONTINUE

    @Milter.noreply
    def body(self, chunk):
        self.fp.write(chunk)
        return Milter.CONTINUE

    def eom(self):
        self.fp.seek(0)
        msg = mime.message_from_file(self.fp)  # Or email. ?
        self.process_attachments(msg)
        return Milter.REJECT

    def process_attachments(self, msg):
        for part in msg.walk():
            attachment_name = ''
            self.log(part.getparams())

            if part.is_multipart():
                continue
            list_of_tuples = part.get_params(None, 'Content-Disposition')  # Check Content-Disposition: filename=
            if list_of_tuples is None:
                if part.get_content_type() == 'text/plain':
                    continue
                list_of_tuples = part.get_params(None)  # Check Content-Type: name=
                if list_of_tuples is None:
                    continue
                for key, value in list_of_tuples:
                    if key.lower() == 'name':
                        attachment_name = value
            else:
                for key, value in list_of_tuples:
                    if key.lower() == 'filename':
                        attachment_name = value
            if attachment_name:
                data = part.get_payload(decode=True)
                #upload_attachment(data)
                self.delete_attachments(part, attachment_name)

        with tempfile.TemporaryFile() as new_msg:
            msg.dump(new_msg)
            new_msg.seek(0)
            while True:
                buf = new_msg.read(8192)
                if len(buf) == 0:
                    break
                self.replacebody(buf)

    def delete_attachments(self, part, attachment_name):
        for key, value in part.get_params():
            part.del_param(key)
        part.set_payload('[Uploaded to S3]\n')

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