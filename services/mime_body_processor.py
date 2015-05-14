import datetime
import sys

from utils.log_config import log_error
from utils.log_config import log_called
from utils.log_config import log_status
from utils import mime
from utils.exceptions import MilterException
from services.s3 import S3
from milter_config import s3_config


def handle_error(function_name, error, name='Postgre Service Error.', params=None):
    error = str(error)
    raise MilterException(500, name, log_error(module_name=__name__, function_name=function_name,
                                               error=error, params=params))


class MimeBodyProcessor(object):

    def __init__(self, log_queue=None, postgre_queue=None, mail_from='', recipients=None):
        self.log_queue = log_queue
        self.postgre_queue = postgre_queue
        self.s3 = None
        self.log = None
        self.attachments = 0
        self.mail_from = mail_from
        self.recipients = [] if recipients is None else ''

    def process_body(self, body):
        self.log('debug', log_called(module_name=__name__, function_name='process_body'))
        html_parts = []
        text_parts = []
        attachments_with_cid = {}
        attachment_generator = mime.get_attachment_parts(body, text_parts, html_parts)
        html_string = '<div><p>Attachments have been uploaded to Amazon S3:</p><ul>'
        text_string = '\nAttachments have been uploaded to Amazon S3:\n'
        for attachment_part, file_name, c_id in attachment_generator:
            try:
                if self.s3 is None:
                    self.s3 = S3(s3_config, self.log_queue)
                data = attachment_part.get_payload(decode=True)
                f_url = self.upload_file(data, file_name)
                self.attachments += 1
                mime.clear_attachment(attachment_part)
                html_string += '<li><a href="{}">{}</a></li>'.format(f_url, file_name)
                text_string += '{}: {}\n'.format(file_name, f_url)
                if c_id is not None:
                    self.log('debug', log_status(module_name=__name__, function_name='process_body',
                                                 msg='CID={}'.format(c_id)))
                    attachments_with_cid[c_id] = f_url
            except Exception as e:
                handle_error('process_body', str(e), params={
                    'file_name': file_name
                })
        if self.attachments == 0:
            return
        try:
            html_string += '</ul></div>'
            mime.add_plain_text_urls(text_parts, text_string)
            mime.replace_cids(html_parts, attachments_with_cid)
            mime.add_html_urls(html_parts, html_string)
        except Exception as e:
            handle_error('process_body', str(e))

    def log_failure(self, error):
        self.log('error', error)
        if self.postgre_queue is not None:
            self.postgre_queue.put({
                'command': {
                    'type': 'insert',
                    'table_name': 'failed_attachment',
                    'columns': ['error', 'sender_id', 'receiver_id']
                },
                'kwargs': {
                    'bind_vars': {
                        'error': error,
                        'sender_id': self.mail_from,
                        'receiver_id': self.recipients[0]
                    }
                }
            })
        else:
            self.log('error', log_error(module_name=__name__, function_name='log_failure',
                                        error='No Postgre Queue Found'))

    def upload_file(self, f_data, f_name):
        self.log('debug', log_called(module_name=__name__, function_name='upload_file', params={
            'f_name': f_name
        }))
        f_folder = datetime.datetime.now().strftime('%Y_%m_%d')
        f_size = sys.getsizeof(f_data, default=None)
        if f_size is None:
            f_size = 0
        url = self.s3.store(path_array=[f_folder], file_name=f_name, data=f_data, config={
            'is_public': True,
            'return_url': True
        })
        if self.postgre_queue is not None:
            for recipient in self.recipients:
                self.log('debug', log_status(module_name=__name__, function_name='upload_file', msg='Sent to Queue'))
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
            self.log('error', log_error(module_name=__name__, function_name='upload_file',
                                        error='No Postgre Queue Found'))
        return url