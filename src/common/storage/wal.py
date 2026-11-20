import os
from pathlib import Path


PENDING = b"\x00"
PROCESSED = b"\x01"


class WriteAheadLog:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        self.file = os.fdopen(fd, "r+b")
        self.path = path

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
        if self.file.read(1) in (b"", PROCESSED):
            return None
        return self.file.read()

    def mark_processed(self):
        self.file.seek(0)
        if self.file.write(PROCESSED) != len(PROCESSED):
            raise OSError("Incomplete WAL status write")
        self._sync()

    def discard(self):
        self.file.truncate(0)
        self._sync()

    def delete(self):
        self.close()
        self.path.unlink(missing_ok=True)

    def close(self):
        self.file.close()
