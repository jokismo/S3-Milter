import datetime
import sys
import math

from utils.log_config import log_error
from utils.log_config import log_called
from utils.log_config import log_status
from utils import mime
from utils.exceptions import MilterException
from services.s3 import S3
from milter_config import s3_config
from milter_config import body_processor_params
from milter_config import html_content
from utils.file_extension_map import reverse_extension_map


def handle_error(function_name, error, name='MimeBodyProcessor Service Error.', params=None):
    if isinstance(error, MilterException):
        raise error
    else:
        error = str(error)
        raise MilterException(500, name, log_error(module_name=__name__, function_name=function_name,
                                                   error=error, params=params))


def update_text_url_string(text_string, file_url, file_name):
    if text_string == '':
        text_string += body_processor_params['plain_text_replace_strings']['initial_line']
    return text_string + body_processor_params['plain_text_replace_strings']['url_line'].format(file_name, file_url)


def shrink_to_max_length(string, max_length):
    length = len(string)
    num_dots = body_processor_params['html']['long_string_num_dots']
    if length > max_length:
        split_position = int(math.floor(max_length / 2))
        return string[:split_position - num_dots] + ('.' * num_dots) + string[length - split_position:]
    else:
        return string


def get_url_string_html(file_url, file_name):
    template_string = html_content['attachment']['html']
    file_ext = file_name.split('.')[-1]
    mime_type = reverse_extension_map.get(file_ext)
    mime_prefix = mime_type.split('/')[0]
    if mime_prefix == 'image':
        icon = 'photo'
    else:
        icon = 'disc'
    file_type_name = shrink_to_max_length(mime_type, 18)
    file_name = shrink_to_max_length(file_name, 18)
    return template_string.format(url=file_url, icon=icon, file_type_name=file_type_name, file_name=file_name)


def compose_attachments_html(url_strings_html):
    num_attachments = len(url_strings_html)
    num_columns = int(math.ceil(float(num_attachments) / 2.0))
    num_rows = int(math.ceil(float(num_columns) / 2.0))
    html = ''
    for z in xrange(0, num_rows):
        if z == 0:
            row_html = html_content['first_row_container']['html']
            row_position = html_content['first_row_container']['insert_position']
        else:
            row_html = html_content['other_row_container']['html']
            row_position = html_content['other_row_container']['insert_position']
        columns_html = ''
        for x in xrange(0, 2):
            if x == 0:
                col_html = html_content['left_col_container']['html']
                left_insert = html_content['left_col_container']['left_insert']
                right_insert = html_content['left_col_container']['right_insert']
            else:
                col_html = html_content['right_col_container']['html']
                left_insert = html_content['right_col_container']['left_insert']
                right_insert = html_content['right_col_container']['right_insert']
            try:
                for y in xrange(0, 2):
                    attachment_html = url_strings_html.pop()
                    if y == 0:
                        position = col_html.find(left_insert)
                        col_html = col_html[:position] + attachment_html + col_html[position:]
                    else:
                        position = col_html.find(right_insert)
                        col_html = col_html[:position] + attachment_html + col_html[position:]
            except IndexError:
                columns_html += col_html
                break
            columns_html += col_html
        row_html = row_html[:row_position] + columns_html + row_html[row_position:]
        html += row_html
    container = html_content['container']['html']
    position = html_content['container']['insert_position']
    return container[:position] + html + container[position:]


class MimeBodyProcessor(object):

    def __init__(self, log_queue=None, postgre_queue=None, mail_from='unknown', recipients=None):
        self.log_queue = log_queue
        self.postgre_queue = postgre_queue
        self.s3 = None
        self.attachments = 0
        self.mail_from = mail_from
        self.recipients = ['unknown'] if recipients is None else ''

    def process_body(self, body):
        self.log('debug', log_called(module_name=__name__, function_name='process_body'))
        html_parts = []
        text_parts = []
        url_strings_html = []
        urls_string_plain_text = ''
        attachments_with_cid = {}
        attachment_generator = mime.get_attachment_parts(body, text_parts, html_parts)
        for attachment_part, file_name, c_id in attachment_generator:
            try:
                if self.s3 is None:
                    self.s3 = S3(s3_config, self.log_queue)
                data = attachment_part.get_payload(decode=True)
                file_url = self.upload_file(data, file_name)
                self.attachments += 1
                mime.clear_attachment(attachment_part)
                update_text_url_string(urls_string_plain_text, file_url, file_name)
                url_strings_html.append(get_url_string_html(file_url, file_name))
                if c_id is not None:
                    self.log('debug', log_status(module_name=__name__, function_name='process_body',
                                                 msg='CID={}'.format(c_id)))
                    attachments_with_cid[c_id] = file_url
            except Exception as e:
                handle_error('process_body', str(e), params={
                    'file_name': file_name,
                    'mail_from': self.mail_from,
                    'recipients': str(self.recipients)
                })
        if self.attachments == 0:
            return
        try:
            mime.add_plain_text_urls(text_parts, urls_string_plain_text)
            mime.replace_cids(html_parts, attachments_with_cid)
            mime.add_html_urls(html_parts, compose_attachments_html(url_strings_html))
        except Exception as e:
            handle_error('process_body', str(e), params={
                'mail_from': self.mail_from,
                'recipients': str(self.recipients)
            })

    def log_failure(self, error):
        self.log('error', error)
        if self.postgre_queue is not None:
            self.postgre_queue.put({
                'command': {
                    'type': 'insert',
                    'table_name': 'failed_attachments',
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

    def log(self, log_type, message):
        if self.log_queue is not None:
            self.log_queue.put({
                'type': log_type,
                'message': message
            })