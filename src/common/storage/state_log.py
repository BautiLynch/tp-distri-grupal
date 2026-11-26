import os

from common.message_protocol.fields import encode_fields, decode_fields
from common.storage.wal import WriteAheadLog, PROCESSED


class StateLog:
    def __init__(self, directory):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self.wals = {}
        self.pending = None

    def save(self, client_id, message, context=0):
        if client_id not in self.wals:
            path = os.path.join(self.directory, f"{client_id}.wal")
            self.wals[client_id] = WriteAheadLog(path)
        record = encode_fields(message, context)
        self.wals[client_id].save(record)

    def mark_processed(self, client_id):
        self.wals[client_id].mark_processed()
        self.pending = None

    def clear(self, client_id):
        wal = self.wals.pop(client_id, None)
        if wal is not None:
            wal.delete()

    def recover(self, restore_state):
        for filename in os.listdir(self.directory):
            if not filename.endswith(".wal"):
                continue
            client_id = int(filename[:-4])
            wal = WriteAheadLog(os.path.join(self.directory, filename))
            self.wals[client_id] = wal
            pending = None
            entry = wal.read()
            while entry is not None:
                status, record = entry
                fields = decode_fields(record)
                if status == PROCESSED:
                    restore_state(fields)
                    pending = None
                else:
                    pending = fields[0]
                entry = wal.read()
            if pending is not None:
                self.pending = pending

    def close(self):
        for wal in self.wals.values():
            wal.close()
