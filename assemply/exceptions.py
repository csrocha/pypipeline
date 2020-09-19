class StopBucket(Exception):
    """
    Exception for Bucket stop.
    """
    def __init__(self, level=0, **kwargs):
        self.level = level
        self.domain = kwargs

    def __eq__(self, other):
        if not isinstance(other, StopBucket):
            return NotImplemented

        return self.level == other.level and self.domain == other.domain


class StopProcess(StopBucket):
    """
    Exception for Process stop.
    """
    def __init__(self):
        super(StopProcess, self).__init__(0)
