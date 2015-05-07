class MilterException(Exception):

    def __init__(self, code, error, full_error=None):
        self.code = code
        self.error = error
        self.full_error = full_error