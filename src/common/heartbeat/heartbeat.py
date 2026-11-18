import logging
import threading

from common import middleware
from common.message_protocol.internal import MsgType, serialize_msg

HEARTBEAT_ROUTING_KEY = "heartbeat"


class HeartbeatSender:
    """Hilo que manda heartbeats al monitor hasta que se llama a stop().

    La conexion a RabbitMQ se crea, usa y cierra dentro del propio hilo
    porque pika no es thread-safe.
    """

    def __init__(self, host, exchange, worker_id, worker_name, interval):
        self.host = host
        self.exchange = exchange
        self.worker_id = int(worker_id)
        self.worker_name = worker_name
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="heartbeat")

    def start(self):
        self._thread.start()

    def stop(self, timeout=None):
        """Frena el hilo y espera a que cierre su conexion."""
        self._stop.set()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout)

    def _run(self):
        exchange = None
        try:
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.host, self.exchange, [HEARTBEAT_ROUTING_KEY], consume=False
            )
            message = serialize_msg(MsgType.HEARTBEAT, self.worker_id, self.worker_name)
            # wait() devuelve True apenas se pide stop, sin esperar el intervalo completo.
            while not self._stop.is_set():
                exchange.send_to(message, routing_key=HEARTBEAT_ROUTING_KEY)
                self._stop.wait(self.interval)
        except Exception:
            # Si no podemos mandar heartbeats el monitor nos va a reiniciar.
            logging.exception("Heartbeat thread failed")
        finally:
            if exchange is not None:
                try:
                    exchange.close()
                except Exception as e:
                    logging.error(f"Error closing heartbeat middleware: {e}")
            logging.info("Heartbeat thread stopped")
