from email import Utils
import datetime
import re

from utils.file_extension_map import extension_map
from utils.log_config import log_error
from utils.exceptions import MilterException


def handle_error(function_name, error, name='Mime Error.'):
    error = str(error)
    raise MilterException(500, name, log_error(module_name=__name__, function_name=function_name,
                                               error=error))


def _check_content_disposition(part, attachment_name):
    try:
        content_disposition = part.get_params(None, 'Content-Disposition')
        if content_disposition is not None:
            is_inline = False
            for key, value in content_disposition:
                if key.lower() == 'filename':
                    attachment_name = value
                if key.lower() == 'inline':
                    is_inline = True
            if is_inline and not attachment_name:
                file_extension = extension_map.get(part['content-type'])
                if file_extension is not None:
                    attachment_name = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S%f') + '.' + file_extension
        return attachment_name
    except Exception as e:
        handle_error('_check_content_disposition', str(e))


def _check_name(part, attachment_name):
    try:
        content_type = part.get_params(None)  # Check Content-Type: name=
        if content_type is not None:
            for key, value in content_type:
                if key.lower() == 'name':
                    attachment_name = value
                    break
        return attachment_name
    except Exception as e:
        handle_error('_check_name', str(e))


def get_attachment_parts(message, txt_list, html_list):
    try:
        for part in message.walk():
            attachment_name = ''
            if part.is_multipart():
                continue
            if part.get_content_type() == 'text/plain':
                txt_list.append(part)
                continue
            if part.get_content_type() == 'text/html':
                html_list.append(part)
                continue
            attachment_name = _check_content_disposition(part, attachment_name)
            if not attachment_name:
                attachment_name = _check_name(part, attachment_name)
            if attachment_name:
                content_id = part['content-id']
                if content_id is not None and '<' in content_id:
                    content_id = content_id[1:-1]
                name_length = len(attachment_name)
                max_length = 128
                if name_length > max_length:
                    attachment_name = attachment_name[name_length - max_length:]
                yield (part, attachment_name, content_id)
    except Exception as e:
        handle_error('get_attachment_parts', str(e))
            
            
def clear_attachment(msg_part):
    try:
        del msg_part['content-type']
        del msg_part['content-disposition']
        del msg_part['content-transfer-encoding']
        del msg_part['content-id']
        msg_part.add_header('Content-Type', 'text/html', charset='UTF-8')
        msg_part.add_header('Content-Disposition', 'inline')
        msg_part.add_header('Content-ID', Utils.make_msgid())
        msg_part.set_payload('\n', charset='UTF-8')
    except Exception as e:
        handle_error('clear_attachment', str(e))
    
    
def _get_decoded_payload(part):
    try:
        payload = part.get_payload(decode=True)
        charset = part.get_content_charset(failobj=None)
        if charset is not None:
            payload = payload.decode(charset)
        return payload
    except Exception as e:
        handle_error('_get_decoded_payload', str(e))
    
    
def add_plain_text_urls(txt_list, urls_string):
    try:
        if len(txt_list) > 0:
            part = txt_list[0]
            text_body = _get_decoded_payload(part)
            text_body = urls_string + '\n' + text_body
            del part['content-transfer-encoding']
            part.set_payload(text_body, charset='utf-8')
    except Exception as e:
        handle_error('add_plain_text_urls', str(e))


def replace_cids(html_list, cid_dict):
    try:
        re_ex = '("cid:).*?(")'
        pattern = re.compile(re_ex, re.IGNORECASE | re.DOTALL)
        for part in html_list:
            payload = _get_decoded_payload(part)
            for match in re.finditer(pattern, payload):
                replace_string = match.group(0)
                cid_text = replace_string[1:-1]
                cid = cid_text[4:]
                if cid_dict.get(cid) is not None:
                    payload = payload.replace(cid_text, cid_dict[cid])
            del part['content-transfer-encoding']
            part.set_payload(payload, charset='utf-8')
    except Exception as e:
        handle_error('replace_cids', str(e))


def add_html_urls(html_list, urls_string):
    try:
        if len(html_list) > 0:
            part = html_list[0]
            html_body = _get_decoded_payload(part)
            position = _get_html_insert_position(html_body)
            html_body = html_body[:position] + urls_string + html_body[position:]
            del part['content-transfer-encoding']
            part.set_payload(html_body, charset='utf-8')
    except Exception as e:
        handle_error('add_html_urls', str(e))


def _get_html_insert_position(html):
    try:
        position = 0
        regex_list = ['(<BODY>)', '(</HEAD>)', '(<).*?(HTML).*?(>)', '(<!DOCTYPE).*?(>)']
        for re_ex in regex_list:
            pattern = re.compile(re_ex, re.IGNORECASE | re.DOTALL)
            match = re.search(pattern, html)
            if match:
                position = match.end()
                break
        return position
    except Exception as e:
        handle_error('_get_html_insert_position', str(e))