class MilterException(Exception):

    def __init__(self, code, error, full_error=None):
        self.code = code
        self.error = error
        self.full_error = full_error

    def __str__(self):
        if self.full_error is not None:
            return str(self.full_error)
        else:
            return self.error