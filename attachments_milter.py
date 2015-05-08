import logging
from multiprocessing import Process as Thread, Queue
import StringIO
import tempfile
import datetime
import sys
import Milter
from email import message_from_file
from email.generator import Generator

from utils import log_config
from utils import mime
from utils.exceptions import MilterException
from services.s3 import S3


log_config.init()
log_config.set_level(is_dev=False)
logger = logging.getLogger(__name__)
log_queue = Queue(maxsize=0)


def log_all_threads():
    while True:
        log_item = log_queue.get()
        if not log_item:
            break
        getattr(logger, log_item['type'])(log_item['message'])


class S3Milter(Milter.Base):

    def __init__(self):  # Each connection calls new S3Milter
        self.id = Milter.uniqueID()  # Integer incremented with each call.
        self.fp = StringIO.StringIO()
        self.s3 = S3()
        self.attachments = 0
        self.start_time = log_config.timestamp_start()

    @Milter.noreply
    def body(self, chunk):
        self.fp.write(chunk)
        return Milter.CONTINUE

    def eom(self):
        self.fp.seek(0)
        try:
            body = message_from_file(self.fp)
            self.process_body(body)
            if self.attachments > 0:
                self.replace_body(body)
                self.log('info', log_config.log_success(module_name=__name__, function_name='eom',
                                                        msg='Upload Complete.',
                                                        params={'count': self.attachments,
                                                                'time': log_config.timestamp_end()}))
        except MilterException as e:
            self.log('error', e.full_error)
        except Exception as e:
            self.log('error', log_config.log_error(module_name=__name__, function_name='eom', error=e))
        finally:
            return Milter.ACCEPT

    def process_body(self, body):
        html_parts = []
        text_parts = []
        attachments_with_cid = {}
        generator = mime.get_attachment_parts(body, text_parts, html_parts)
        html_string = '<div><p>Attachments have been uploaded to Amazon S3:</p><ul>'
        text_string = '\nAttachments have been uploaded to Amazon S3:\n'
        for attachment_part, file_name, c_id in generator:
            data = attachment_part.get_payload(decode=True)
            f_url = self.upload_file(data, file_name)
            mime.clear_attachment(attachment_part)
            html_string += '<li><a href="{}">{}</a></li>'.format(f_url, file_name)
            text_string += '{}: {}\n'.format(file_name, f_url)
            if c_id is not None:
                attachments_with_cid[c_id] = f_url
        if self.attachments == 0:
            return
        html_string += '</ul></div>'
        mime.add_plain_text_urls(text_parts, text_string)
        mime.replace_cids(html_parts, attachments_with_cid)
        mime.add_html_urls(html_parts, html_string)

    def upload_file(self, f_data, f_name):
        self.attachments += 1
        f_folder = datetime.datetime.now().strftime('%Y_%m_%d')
        f_size = sys.getsizeof(f_data, default=None)
        if f_size is None:
            f_size = 0
        url = self.s3.store(path_array=[f_folder], key=f_name, data=f_data)
        return url

    def replace_body(self, msg):
        with tempfile.TemporaryFile() as msg_file:
            g = Generator(msg_file)
            g.flatten(msg)
            msg_file.seek(0)
            while True:
                buf = msg_file.read(8192)
                if len(buf) == 0:
                    break
                self.replacebody(buf)

    def abort(self):
        self.log('error', log_config.log_error(module_name=__name__, function_name='abort',
                                               error='Client disconnected prematurely.'))
        return Milter.CONTINUE

    def log(self, log_type, message):
        log_queue.put({
            'type': log_type,
            'message': message
        })


def execute():
    logger_thread = Thread(target=log_all_threads)
    logger_thread.start()
    socket_name = '/etc/s3milter/sock'
    timeout = 600
    Milter.factory = S3Milter
    flags = Milter.MODBODY
    Milter.set_flags(flags)
    logger.info(log_config.log_success(module_name=__name__, function_name='execute',
                                       msg='S3 Milter service launched.'))
    Milter.runmilter('S3Milter', socket_name, timeout)
    log_queue.put(None)
    logger_thread.join()
    logger.info(log_config.log_success(module_name=__name__, function_name='execute',
                                       msg='S3 Milter service shutdown.'))

if __name__ == "__main__":
    execute()