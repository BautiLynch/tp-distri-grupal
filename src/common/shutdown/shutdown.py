import logging
import signal
import threading


class GracefulShutdown:
    """Ciclo de vida de un worker: frena ante SIGTERM y cierra todo."""

    def __init__(self):
        self.requested = threading.Event()
        self._consumer = None
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, signum, frame):
        self.requested.set()
        if self._consumer is not None:
            self._consumer.stop_consuming_threadsafe()

    def run(self, consumer, on_message, close):
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
