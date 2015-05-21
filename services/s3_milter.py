import Milter
from Milter.utils import parse_addr
from email import message_from_file
from email.generator import Generator
import StringIO
import tempfile

from utils.log_config import log_error
from utils.log_config import log_success
from utils.log_config import log_called
from utils.log_config import log_status
from utils.log_config import timestamp_start
from utils.log_config import timestamp_end
from utils.exceptions import MilterException
from services.mime_body_processor import MimeBodyProcessor


def handle_error(function_name, error, name='S3Milter Service Error.', params=None):
    error = str(error)
    raise MilterException(500, name, log_error(module_name=__name__, function_name=function_name,
                                               error=error, params=params))


class S3Milter(MimeBodyProcessor, Milter.Base):

    def __init__(self):  # Each connection calls new S3Milter
        MimeBodyProcessor.__init__(self)
        self.id = Milter.uniqueID()  # Integer incremented with each call.
        self.fp = StringIO.StringIO()
        self.log_queue = None
        self.postgre_queue = None
        self.start_time = timestamp_start()
        self.log('debug', log_status(module_name=__name__, function_name='__init__',
                                     msg='Launched Milter ID={}'.format(self.id)))

    @Milter.noreply
    def envfrom(self, mail_from, *esmtp_params):
        self.log('debug', log_called(module_name=__name__, function_name='envfrom', params={
            'mail_from': str(mail_from)
        }))
        try:
            self.mail_from = parse_addr(mail_from)[0]
        except Exception:
            self.mail_from = str(mail_from)
        return Milter.CONTINUE

    @Milter.noreply
    def envrcpt(self, mail_recip, *esmtp_params):
        self.log('debug', log_called(module_name=__name__, function_name='envrcpt', params={
            'mail_recip': str(mail_recip)
        }))
        try:
            recip = parse_addr(mail_recip)[0]
        except Exception:
            recip = str(mail_recip)
        self.recipients.append(recip)
        return Milter.CONTINUE

    @Milter.noreply
    def body(self, chunk):
        self.log('debug', log_called(module_name=__name__, function_name='body'))
        self.fp.write(chunk)
        return Milter.CONTINUE

    def eom(self):
        self.log('debug', log_called(module_name=__name__, function_name='eom'))
        self.fp.seek(0)
        try:
            body = message_from_file(self.fp)
            self.process_body(body)
            if self.attachments > 0:
                self.replace_body(body)
                self.log('info', log_success(module_name=__name__, function_name='eom',
                                             msg='Upload Complete.',
                                             params={'count': self.attachments,
                                                     'time': timestamp_end(self.start_time)}))
        except MilterException as e:
            self.log_failure(str(e))
        except Exception as e:
            self.log_failure(log_error(module_name=__name__, function_name='eom', error=str(e), params={
                'mail_from': self.mail_from,
                'recipients': str(self.recipients)
            }))
        finally:
            return Milter.ACCEPT

    def replace_body(self, msg):
        self.log('debug', log_called(module_name=__name__, function_name='replace_body'))
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
        self.log('error', log_error(module_name=__name__, function_name='abort',
                                    error='Client disconnected prematurely.'))
        return Milter.CONTINUE

    def log(self, log_type, message):
        if self.log_queue is not None:
            self.log_queue.put({
                'type': log_type,
                'message': message
            })