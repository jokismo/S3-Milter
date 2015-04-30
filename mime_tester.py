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
                attachment_name = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '.' + file_extension
    return attachment_name


def check_name(part, attachment_name):
    content_type = part.get_params(None)  # Check Content-Type: name=
    if content_type is not None:
        for key, value in content_type:
            if key.lower() == 'name':
                attachment_name = value
                break
    return attachment_name


def get_attachment_parts(message, html_list):
    for part in message.walk():
        attachment_name = ''
        if part.is_multipart():
            continue
        if part.get_content_type() == 'text/plain':
            continue
        if part.get_content_type() == 'text/html':
            html_list.append(part)
            body = part.get_payload()
            print 'cid' in body
            continue
        attachment_name = check_content_disposition(part, attachment_name)
        if not attachment_name:
            attachment_name = check_name(part, attachment_name)
        if attachment_name:
            content_id = part['content-id']
            if content_id is not None and '<' in content_id:
                content_id = content_id[1:-1]
                print content_id
            yield (part, attachment_name)


def upload_file(f_data, f_name):
    pass


def clear_attachment(msg_part):
    del msg_part['content-type']
    del msg_part['content-disposition']
    del msg_part['content-transfer-encoding']
    del msg_part['content-id']
    msg_part.add_header('Content-Type', 'text/html', charset='UTF-8')
    msg_part.add_header('Content-Disposition', 'inline')
    msg_part.add_header('Content-ID', Utils.make_msgid())
    msg_part.set_payload('Attachment uploaded to S3\n')

if __name__ == '__main__':
    f = open('sample_message', 'r')
    try:
        html_parts = []
        msg = message_from_file(f)
        generator = get_attachment_parts(msg, html_parts)
        for attachment_part, file_name in generator:
            data = attachment_part.get_payload(decode=True)
            upload_file(data, file_name)
            clear_attachment(attachment_part)

        txt = '<img src="cid:smile@here" alt="smile"><img src="cid:pen@here" alt="smile">'
        re1 = '(")'	 # Any Single Character 1
        re2 = '(cid)'  # Word 1
        re3 = '(:)'	 # Any Single Character 2
        re4 = '.*?'	 # Non-greedy match on filler
        re5 = '(")'	 # Any Single Character 3

        pattern = re.compile(re1 + re2 + re3 + re4 + re5, re.IGNORECASE | re.DOTALL)
        position = 0
        found = True
        while found:
            found = pattern.search(txt, position)
            if found is not None:
                position = found.end()
                replace_string = found.group(0)
                cid = replace_string[5:-1]
                print txt[found.start():found.end()]
                print found.start()
                print found.end()
                txt = txt.replace(found.group(0), 'ppp')
        # found = rg.search(txt, found.end())
        # txt = re.sub(rg, 'pen', txt)
        print txt
        print 'aaaaa'.replace('a', 'b')
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