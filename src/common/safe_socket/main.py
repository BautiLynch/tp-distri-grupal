import socket

def recv_all(socket: socket.socket, size):
    data = bytearray()
    while len(data) < size:
        chunk = socket.recv(size - len(data))
        if not chunk:
            return bytes(data)
        data += chunk
    return bytes(data)


def send_all(socket: socket.socket, data):
    view = memoryview(data)
    while len(view):
        sent = socket.send(view)
        view = view[sent:]