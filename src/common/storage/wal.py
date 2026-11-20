import os
from pathlib import Path


PENDING = b"\x00"
SENT = b"\x01"


class WriteAheadLog:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        self.file = os.fdopen(fd, "r+b")

    def _sync(self):
        self.file.flush()
        os.fsync(self.file.fileno())

    def save(self, message):
        self.file.seek(0)
        self.file.truncate()
        record = PENDING + message
        if self.file.write(record) != len(record):
            raise OSError("Incomplete WAL write")
        self._sync()

    def load(self):
        self.file.seek(0)
        if self.file.read(1) in (b"", SENT):
            return None
        return self.file.read()

    def mark_sent(self):
        self.file.seek(0)
        if self.file.write(SENT) != len(SENT):
            raise OSError("Incomplete WAL status write")
        self._sync()

    def discard(self):
        self.file.truncate(0)
        self._sync()

    def close(self):
        self.file.close()
