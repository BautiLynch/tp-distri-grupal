import logging
import queue
import threading
import time

from common import middleware
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType


def gossip_key(monitor_id):
    return f"gossip_monitor_{monitor_id}"


class Gossip:
    def __init__(
        self, my_id, amount, host, exchange, interval, timeout, container_id, *,
        current_containers, on_containers, on_election_message, on_peer_timeout,
        on_failure,
    ):
        self.my_id = my_id
        self.amount = amount
        self.host = host
        self.exchange = exchange
        self.interval = interval
        self.timeout = timeout
        self.container_id = container_id

        self.current_containers = current_containers    # () -> [(container_id, worker_name, age_ms)]
        self.on_containers = on_containers              # (sender, [(container_id, worker_name, age_ms)])
        self.on_election_message = on_election_message  # (msg_type, sender)
        self.on_peer_timeout = on_peer_timeout          # (peer_id, container_id)
        self.on_failure = on_failure

        self._condition = threading.Condition()
        self._stopping = False
        self._ready = False
        self._peers = {}

        self._consumer = None
        self._outbox = queue.Queue()
        self._threads = [
            threading.Thread(target=self._run_listener, name="gossip-listener"),
            threading.Thread(target=self._run_sender, name="gossip-sender"),
            threading.Thread(target=self._watch_peers, name="gossip-watchdog"),
        ]

    def start(self):
        for thread in self._threads:
            thread.start()

    def wait_ready(self):
        with self._condition:
            self._condition.wait_for(lambda: self._stopping or self._ready)
            return not self._stopping

    def close(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
            consumer = self._consumer

        if consumer is not None:
            consumer.stop_consuming_threadsafe()
        self._outbox.put(None)

        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=self.timeout)

    def send_election(self, msg_type, target):
        self._outbox.put((serialize_msg(msg_type, self.my_id), target))

    def _run_sender(self):
        publisher = None
        try:
            publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.host, self.exchange, [], consume=False
            )
            next_gossip = time.monotonic()

            while True:
                now = time.monotonic()
                if now >= next_gossip:
                    gossip = serialize_msg(
                        MsgType.GOSSIP, self.my_id, self.container_id, self.current_containers()
                    )
                    for index in range(self.amount):
                        if index != self.my_id:
                            publisher.send_to(gossip, gossip_key(index))
                    next_gossip = now + self.interval

                try:
                    item = self._outbox.get(timeout=max(0, next_gossip - time.monotonic()))
                except queue.Empty:
                    continue

                if item is None:
                    return
                message, target = item
                publisher.send_to(message, gossip_key(target))

        except Exception:
            if not self._stopping:
                logging.exception("Gossip sender failed")
                self.on_failure()

        finally:
            if publisher is not None:
                try:
                    publisher.close()
                except Exception as e:
                    logging.error(f"Error closing gossip publisher: {e}")

    def _run_listener(self):
        consumer = None
        try:
            consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.host, self.exchange, [gossip_key(self.my_id)], consume=True,
            )

            with self._condition:
                if self._stopping:
                    return
                self._consumer = consumer
                self._ready = True
                self._condition.notify_all()

            consumer.start_consuming(self._process_message)

        except Exception:
            if not self._stopping:
                logging.exception("Gossip listener failed")
                self.on_failure()

        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as e:
                    logging.error(f"Error closing gossip consumer: {e}")

    def _process_message(self, message, ack, nack):
        try:
            msg_type, fields = deserialize_msg(message)
            sender = fields[0]

            if msg_type == MsgType.GOSSIP:
                _, container_id, containers = fields
                logging.debug(f"Gossip from monitor {sender} (container {container_id})")
                with self._condition:
                    self._peers[sender] = (container_id, time.monotonic() + self.timeout)
                    self._condition.notify_all()
                self.on_containers(sender, containers)
            else:
                self.on_election_message(msg_type, sender)

        except Exception as e:
            logging.error(f"Invalid monitor message: {e}")
        ack()

    def _watch_peers(self):
        while True:
            with self._condition:
                if self._stopping:
                    return

                if not self._peers:
                    self._condition.wait()
                    continue

                peer = min(self._peers, key=lambda p: self._peers[p][1])
                container_id, deadline = self._peers[peer]
                remaining = deadline - time.monotonic()

                if remaining > 0:
                    self._condition.wait(timeout=remaining)
                    continue

                self._peers[peer] = (container_id, time.monotonic() + self.timeout)

            logging.debug(f"Monitor {peer} timeout")
            self.on_peer_timeout(peer, container_id)
