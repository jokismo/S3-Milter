from email import message_from_file
import sys

from services.s3 import S3
from utils import mime


def upload_file(f_data, f_name, service):
    print sys.getsizeof(f_data, default=0)
    # url = service.store(path_array=['test'], key=f_name, data=f_data)
    # return url
    return 'mock_url'

if __name__ == '__main__':
    s3 = S3()
    s3.set_bucket('persafd')
    f = open('sample_message', 'r')
    try:
        html_parts = []
        text_parts = []
        attachments_with_cid = {}
        msg = message_from_file(f)
        generator = mime.get_attachment_parts(msg, text_parts, html_parts)
        html_string = '<div><p>Attachments have been uploaded to Amazon S3:</p><ul>'
        text_string = '\nAttachments have been uploaded to Amazon S3:\n'
        for attachment_part, file_name, c_id in generator:
            data = attachment_part.get_payload(decode=True)
            f_url = upload_file(data, file_name, s3)
            mime.clear_attachment(attachment_part)
            html_string += '<li><a href="{}">{}</a></li>'.format(f_url, file_name)
            text_string += '{}: {}\n'.format(file_name, f_url)
            if c_id is not None:
                attachments_with_cid[c_id] = f_url
        html_string += '</ul></div>'
        mime.add_plain_text_urls(text_parts, text_string)
        mime.replace_cids(html_parts, attachments_with_cid)
        mime.add_html_urls(html_parts, html_string)
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