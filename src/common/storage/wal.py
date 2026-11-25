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
        self.position = 0

    def _sync(self):
        self.file.flush()
        os.fsync(self.file.fileno())

    def save(self, message):
        self.position = self.file.seek(0, os.SEEK_END)
        record = PENDING + len(message).to_bytes(4, "big") + message
        if self.file.write(record) != len(record):
            raise OSError("Incomplete WAL write")
        self._sync()

    def load(self):
        self.file.seek(0)
        last = None
        record = self.read()
        while record is not None:
            status, message = record
            last = message if status == PENDING else None
            record = self.read()
        return last

    def read(self):
        position = self.file.tell()
        header = self.file.read(5)
        if not header:
            return None
        if len(header) != 5 or header[:1] not in (PENDING, PROCESSED):
            self.file.truncate(position)
            self._sync()
            return None
        size = int.from_bytes(header[1:], "big")
        message = self.file.read(size)
        if len(message) != size:
            self.file.truncate(position)
            self._sync()
            return None
        self.position = position
        return header[:1], message

    def mark_processed(self):
        self.file.seek(self.position)
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
