class StopBucket(Exception):
    """
    Exception for Bucket stop.
    """
    def __init__(self, level=0):
        self.level = level

    def __eq__(self, other):
        if not isinstance(other, StopBucket):
            return NotImplemented

        return self.level == other.level


class StopProcess(StopBucket):
    """
    Exception for Process stop.
    """
    def __init__(self):
        super(StopProcess, self).__init__(0)
