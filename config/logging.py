import os
import logging
from logging.handlers import RotatingFileHandler
from config.settings import config


class LoggingConfig:
    """
    Klasa LoggingConfig służy do konfiguracji logowania w aplikacji.

    Ta klasa tworzy i konfiguruje loggera z RotatingFileHandler, co pozwala na
    obracanie plików logów po osiągnięciu określonego rozmiaru. Jest ona zaprojektowana
    jako klasa jednoelementowa (singleton), aby zapewnić globalny dostęp do tego samego
    loggera w całej aplikacji.
    """

    _instance = None

    def __new__(cls):
        """
        Tworzy instancję klasy LoggingConfig, jeśli jeszcze nie istnieje.
        """
        if cls._instance is None:
            cls._instance = super(LoggingConfig, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """
        Inicjalizuje loggera, jeśli nie został jeszcze zainicjalizowany.
        """
        if self._initialized:
            return

        self._setup_logging()
        self._initialized = True

    def _setup_logging(self):
        """
        Konfiguruje loggera, ustawiając format, poziom logowania i dodając RotatingFileHandler.
        """
        logs_directory = config.LOG_DIRECTORY
        if not os.path.exists(logs_directory):
            os.makedirs(logs_directory)

        log_file = config.LOG_FILE

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                RotatingFileHandler(log_file, maxBytes=100000000, backupCount=5),
                logging.StreamHandler(),
            ],
        )

    @staticmethod
    def get_logger():
        """
        Zwraca zainicjalizowanego loggera do użytku w aplikacji.
        """
        return logging.getLogger()