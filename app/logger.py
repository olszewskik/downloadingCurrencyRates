from config.logging import LoggingConfig


class Logger:
    """
    Klasa Logger służy do logowania różnych typów wiadomości w aplikacji.
    Wykorzystuje konfigurację zdefiniowaną w LoggingConfig.

    Metody:
    - log_start: Loguje wiadomość informacyjną o rozpoczęciu jakiegoś procesu.
    - log_error: Loguje wiadomość o błędzie wraz ze szczegółami.
    - log_success: Loguje wiadomość informacyjną o pomyślnym zakończeniu operacji.
    """

    def __init__(self) -> None:
        """
        Inicjalizuje klasę Logger, konfigurując logowanie.
        """
        LoggingConfig()
        self.logger = LoggingConfig.get_logger()

    def log_start(self, message):
        """
        Loguje informację o rozpoczęciu czegoś, na przykład procesu lub zadania.

        Args:
            message (str): Wiadomość do zalogowania.
        """
        self.logger.info(message)

    def log_error(self, message, error):
        """
        Loguje błąd z dodatkowymi szczegółami.

        Args:
            message (str): Szablon wiadomości do zalogowania.
            error (Exception): Wyjątek lub informacja o błędzie do zalogowania.
        """
        self.logger.error(message.format(error=error))

    def log_success(self, message):
        """
        Loguje informację o pomyślnym zakończeniu operacji.

        Args:
            message (str): Wiadomość do zalogowania.
        """
        self.logger.info(message)