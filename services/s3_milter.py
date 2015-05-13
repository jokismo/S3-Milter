import Milter
from Milter.utils import parse_addr
from email import message_from_file
from email.generator import Generator
import StringIO
import tempfile
import datetime
import sys

from utils import log_config
from utils import mime
from utils.exceptions import MilterException
from services.s3 import S3


def handle_error(function_name, error, name='Postgre Service Error.'):
    error = str(error)
    raise MilterException(500, name, log_config.log_error(module_name=__name__, function_name=function_name,
                                                          error=error))


class S3Milter(Milter.Base):

    def __init__(self):  # Each connection calls new S3Milter
        self.id = Milter.uniqueID()  # Integer incremented with each call.
        self.fp = StringIO.StringIO()
        self.s3 = None
        self.log_queue = None
        self.postgre_queue = None
        self.attachments = 0
        self.start_time = log_config.timestamp_start()
        self.mail_from = ''
        self.recipients = []

    @Milter.noreply
    def envfrom(self, mail_from, *esmtp_params):
        try:
            self.mail_from = parse_addr(mail_from)[0]
        except Exception:
            self.mail_from = str(mail_from)
        return Milter.CONTINUE

    @Milter.noreply
    def envrcpt(self, mail_recip, *esmtp_params):
        try:
            recip = parse_addr(mail_recip)[0]
        except Exception:
            recip = str(mail_recip)
        self.recipients.append(recip)
        return Milter.CONTINUE

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
            self.log('error', str(e))
        except Exception as e:
            self.log('error', log_config.log_error(module_name=__name__, function_name='eom', error=str(e)))
        finally:
            return Milter.ACCEPT

    def process_body(self, body):
        f_url = ''
        html_parts = []
        text_parts = []
        attachments_with_cid = {}
        attachment_generator = mime.get_attachment_parts(body, text_parts, html_parts)
        html_string = '<div><p>Attachments have been uploaded to Amazon S3:</p><ul>'
        text_string = '\nAttachments have been uploaded to Amazon S3:\n'
        for attachment_part, file_name, c_id in attachment_generator:
            try:
                if self.s3 is None:
                    self.s3 = S3()
                data = attachment_part.get_payload(decode=True)
                f_url = self.upload_file(data, file_name)
                mime.clear_attachment(attachment_part)
                html_string += '<li><a href="{}">{}</a></li>'.format(f_url, file_name)
                text_string += '{}: {}\n'.format(file_name, f_url)
                if c_id is not None:
                    attachments_with_cid[c_id] = f_url
            except MilterException as e:
                self.log_failure(file_name, f_url, str(e))
                raise e
            except Exception as e:
                self.log_failure(file_name, f_url, str(e))
                handle_error('process_body', str(e))
        if self.attachments == 0:
            return
        try:
            html_string += '</ul></div>'
            mime.add_plain_text_urls(text_parts, text_string)
            mime.replace_cids(html_parts, attachments_with_cid)
            mime.add_html_urls(html_parts, html_string)
        except MilterException as e:
            self.log_failure('Post Process', f_url, str(e))
            raise e
        except Exception as e:
            self.log_failure('Post Process', f_url, str(e))
            handle_error('process_body', str(e))

    def log_failure(self, f_name, f_url, error):
        if self.postgre_queue is not None:
            self.postgre_queue.put({
                'command': {
                    'type': 'insert',
                    'table_name': 'failed_attachment',
                    'columns': ['file_name', 'error', 'sender_id', 'receiver_id', 'url']
                },
                'kwargs': {
                    'bind_vars': {
                        'file_name': f_name,
                        'error': error,
                        'sender_id': self.mail_from,
                        'receiver_id': self.recipients[0],
                        'url': f_url
                    }
                }
            })
        else:
            self.log('error', log_config.log_error(module_name=__name__, function_name='log_failure',
                                                   error='No Postgre Queue Found'))

    def upload_file(self, f_data, f_name):
        self.attachments += 1
        f_folder = datetime.datetime.now().strftime('%Y_%m_%d')
        f_size = sys.getsizeof(f_data, default=None)
        if f_size is None:
            f_size = 0
        url = self.s3.store(path_array=[f_folder], key=f_name, data=f_data)
        if self.postgre_queue is not None:
            for recipient in self.recipients:
                self.postgre_queue.put({
                    'command': {
                        'type': 'insert',
                        'table_name': 'attachments',
                        'columns': ['file_name', 'folder', 'sender_id', 'receiver_id', 'bytes']
                    },
                    'kwargs': {
                        'bind_vars': {
                            'file_name': f_name,
                            'folder': f_folder,
                            'sender_id': self.mail_from,
                            'receiver_id': recipient,
                            'bytes': f_size
                        }
                    }
                })
        else:
            self.log('error', log_config.log_error(module_name=__name__, function_name='upload_file',
                                                   error='No Postgre Queue Found'))
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
        self.log_queue.put({
            'type': log_type,
            'message': message
        })