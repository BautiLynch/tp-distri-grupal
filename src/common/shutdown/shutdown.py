import logging
import signal
import threading


class GracefulShutdown:
    """Ciclo de vida de un worker: frena ante SIGTERM/SIGINT y cierra todo.

    Hay que crearlo antes de conectarse a RabbitMQ para cubrir tambien el
    arranque.
    """

    def __init__(self):
        self.requested = threading.Event()
        self._consumer = None
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

    def _on_signal(self, signum, frame):
        self.requested.set()
        if self._consumer is not None:
            self._consumer.stop_consuming_threadsafe()

    def run(self, consumer, on_message, close):
        """Consume hasta que se pida el apagado y despues cierra todo."""
        self._consumer = consumer
        if self.requested.is_set():
            consumer.stop_consuming_threadsafe()
        try:
            consumer.start_consuming(on_message)
        except Exception:
            if not self.requested.is_set():
                raise
            logging.info("Consumer stopped during shutdown")
        finally:
            close()
