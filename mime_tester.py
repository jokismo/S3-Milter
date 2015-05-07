from email import message_from_file
from email import Message
from email import Generator
from email import Utils
import datetime
import re

from file_extension_map import extension_map


def check_content_disposition(part, attachment_name):
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


def check_name(part, attachment_name):
    content_type = part.get_params(None)  # Check Content-Type: name=
    if content_type is not None:
        for key, value in content_type:
            if key.lower() == 'name':
                attachment_name = value
                break
    return attachment_name


def get_attachment_parts(message, txt_list, html_list):
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
        attachment_name = check_content_disposition(part, attachment_name)
        if not attachment_name:
            attachment_name = check_name(part, attachment_name)
        if attachment_name:
            content_id = part['content-id']
            if content_id is not None and '<' in content_id:
                content_id = content_id[1:-1]
            yield (part, attachment_name, content_id)


def upload_file(f_data, f_name):
    url = 'http://url/' + f_name
    return url


def clear_attachment(msg_part):
    del msg_part['content-type']
    del msg_part['content-disposition']
    del msg_part['content-transfer-encoding']
    del msg_part['content-id']
    msg_part.add_header('Content-Type', 'text/html', charset='UTF-8')
    msg_part.add_header('Content-Disposition', 'inline')
    msg_part.add_header('Content-ID', Utils.make_msgid())
    msg_part.set_payload('\n', charset='UTF-8')


def get_decoded_payload(part):
    payload = part.get_payload(decode=True)
    charset = part.get_content_charset(failobj=None)
    if charset is not None:
        payload = payload.decode(charset)
    return payload
    
    
def add_plain_text_urls(txt_list, urls_string):
    if len(txt_list) > 0:
        text_body = get_decoded_payload(text_parts[0])
        text_body = urls_string + '\n' + text_body
        txt_list[0].set_payload(text_body, charset='utf-8')


def replace_cids(html_list, cid_dict):
    re_ex = '("cid:).*?(")'
    pattern = re.compile(re_ex, re.IGNORECASE | re.DOTALL)
    for part in html_list:
        payload = get_decoded_payload(part)
        for match in re.finditer(pattern, payload):
            replace_string = match.group(0)
            cid_text = replace_string[1:-1]
            cid = cid_text[4:]
            if cid_dict.get(cid) is not None:
                payload = payload.replace(cid_text, cid_dict[cid])
        part.set_payload(payload, charset='utf-8')


def add_html_urls(html_list, urls_string):
    if len(html_list) > 0:
        part = html_list[0]
        html_body = get_decoded_payload(part)
        position = get_html_insert_position(html_body)
        html_body = html_body[:position] + urls_string + html_body[position:]
        del part['content-transfer-encoding']
        part.set_payload(html_body, charset='utf-8')


def get_html_insert_position(html):
    position = 0
    regex_list = ['(<BODY>)', '(</HEAD>)', '(<).*?(HTML).*?(>)', '(<!DOCTYPE).*?(>)']
    for re_ex in regex_list:
        pattern = re.compile(re_ex, re.IGNORECASE | re.DOTALL)
        match = re.search(pattern, html)
        if match:
            position = match.end()
            break
    return position


if __name__ == '__main__':
    f = open('sample_messagee', 'r')
    try:
        html_parts = []
        text_parts = []
        attachments_with_cid = {}
        msg = message_from_file(f)
        generator = get_attachment_parts(msg, text_parts, html_parts)
        html_string = '<div><p>Attachments have been uploaded to Amazon S3:</p><ul>'
        text_string = '\nAttachments have been uploaded to Amazon S3:\n'
        for attachment_part, file_name, c_id in generator:
            data = attachment_part.get_payload(decode=True)
            f_url = upload_file(data, file_name)
            clear_attachment(attachment_part)
            html_string += '<li><a href="{}">{}</a></li>'.format(f_url, file_name)
            text_string += '{}: {}\n'.format(file_name, f_url)
            if c_id is not None:
                attachments_with_cid[c_id] = f_url
        html_string += '</ul></div>'
        add_plain_text_urls(text_parts, text_string)
        replace_cids(html_parts, attachments_with_cid)
        add_html_urls(html_parts, html_string)
        print msg
    finally:
        f.close()


# class MimeMessage(Message):
#
#     def __init__(self):
#         Message.__init__(self)
#
#     def dump(self, output_file, unixfrom=False):
#         g = Generator(output_file)
#         g.flatten(self, unixfrom=unixfrom)