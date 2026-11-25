"""Eleccion de lider con el algoritmo Bully: gana el monitor vivo de ID mas alto."""
import logging
import threading

from common.message_protocol.internal import MsgType


class BullyElection:
    def __init__(self, my_id, amount, timeout, send, on_leader_change):
        self.my_id = my_id
        self.amount = amount
        self.timeout = timeout
        self.send = send
        self.on_leader_change = on_leader_change

        self._condition = threading.Condition()
        self._leader = None
        self._requested = True
        self._in_progress = False
        self._answered = False
        self._stopping = False

        self._notify_lock = threading.Lock()
        self._published = None

        self._thread = threading.Thread(target=self._run, name="election")


    @property
    def leader_id(self):
        with self._condition:
            return self._leader

    def start(self):
        self._thread.start()

    def close(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        if self._thread.is_alive():
            self._thread.join()

    def leader_lost(self, leader):
        """El gossip dejo de recibir al lider."""
        with self._condition:
            if self._leader != leader:
                return
            logging.info(f"Leader {leader} is down")
            self._leader = None
            self._request_locked()
        self._publish()

    def handle(self, msg_type, sender):
        if msg_type == MsgType.ELECTION:
            self._on_election(sender)
        elif msg_type == MsgType.ELECTION_OK:
            self._on_election_ok(sender)
        elif msg_type == MsgType.COORDINATOR:
            self._on_coordinator(sender)

    def _on_election(self, sender):
        logging.info(f"Election message from monitor {sender}")
        self.send(MsgType.ELECTION_OK, sender)
        with self._condition:
            self._request_locked()

    def _on_election_ok(self, sender):
        with self._condition:
            if self._in_progress:
                self._answered = True
                self._condition.notify_all()

    def _on_coordinator(self, sender):
        reply = False
        with self._condition:
            if sender < self.my_id:
                if self._leader == self.my_id:
                    reply = True
                else:
                    self._request_locked()
            else:
                self._set_leader_locked(sender)

        if reply:
            self.send(MsgType.COORDINATOR, sender)
        self._publish()

    def _request_locked(self):
        if self._in_progress or self._requested:
            return
        self._requested = True
        self._condition.notify_all()

    def _set_leader_locked(self, leader):
        if leader != self._leader:
            logging.info(f"Monitor {leader} is the leader")
        self._leader = leader
        self._in_progress = False
        self._requested = False
        self._condition.notify_all()

    def _publish(self):
        with self._notify_lock:
            leader = self.leader_id
            if leader != self._published:
                self._published = leader
                self.on_leader_change(leader)

    def _broadcast(self, msg_type):
        for index in range(self.amount):
            if index != self.my_id:
                self.send(msg_type, index)

    def _run(self):
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._stopping or self._requested)
                if self._stopping:
                    return
                self._requested = False
                self._in_progress = True
                self._answered = False

            logging.info("Starting election")
            for index in range(self.my_id + 1, self.amount):
                self.send(MsgType.ELECTION, index)

            won = False
            with self._condition:
                if self.my_id + 1 < self.amount:
                    self._condition.wait_for(
                        lambda: self._stopping or self._answered or not self._in_progress,
                        timeout=self.timeout,
                    )
                if self._stopping:
                    return
                if not self._in_progress:
                    pass  # Llego un COORDINATOR mientras esperaba.
                elif not self._answered:
                    logging.info("No higher monitor answered: I am the leader")
                    self._set_leader_locked(self.my_id)
                    won = True
                else:
                    self._condition.wait_for(
                        lambda: self._stopping or not self._in_progress,
                        timeout=2 * self.timeout,
                    )
                    if self._stopping:
                        return
                    if self._in_progress:
                        logging.info("No coordinator received: retrying election")
                        self._in_progress = False
                        self._requested = True

            if won:
                self._broadcast(MsgType.COORDINATOR)
            self._publish()
