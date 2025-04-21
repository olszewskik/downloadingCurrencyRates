from .logger import Logger
from config.settings import Config
from db.engine import DbEngine
from db.db_manager import DatabaseManager
from app.clients.nbp_api_client import NBPApiClient
from app.loaders.csv_loader import CSVRateLoader
from app.services.exchange_rate_saver import ExchangeRateSaver
from app.services.exchange_rate_manager import ExchangeRateManager


class Application:
    """
    Główna klasa aplikacji do pobierania i zapisywania kursów walut.
    """
    def __init__(self):
        # Konfiguracja
        self.config = Config()
        self.logger = Logger()

        # Baza danych
        self.db_engine = DbEngine()
        self.db_manager = DatabaseManager(self.db_engine)

        # Komponenty logiki biznesowej
        self.api_client = NBPApiClient(self.config.NBP_API_BASE_URL)
        self.csv_loader = CSVRateLoader(self.config.NBP_CSV_BASE_URL)
        self.saver = ExchangeRateSaver(self.db_manager)
        self.manager = ExchangeRateManager(
            api_client=self.api_client,
            csv_loader=self.csv_loader,
            saver=self.saver
        )

    def run_daily_only(self, start_date: str, end_date: str):
        """
        Uruchamia synchronizację tylko kursów dziennych.
        """
        self.manager.sync_daily_rates(start_date, end_date)

    def run_monthly_only(self, year: int):
        """
        Uruchamia synchronizację tylko kursów miesięcznych.
        """
        self.manager.sync_monthly_rates(year)

    def run_cumulative_only(self, year: int):
        """
        Uruchamia synchronizację tylko kursów narastających.
        """
        self.manager.sync_cumulative_rates(year)

    def run_sync(self, year: int):
        """
        Uruchamia synchronizację wszystkich kursów.
        """
        self.logger.log_start(self.config.LOG_STARTING_APP_MSG)
        try:
            self.manager.sync_all(year)
            self.logger.log_success(self.config.LOG_FINISHED_APP_SUCCESS_MSG)
        except SystemExit as e:
            self.logger.log_error(self.config.LOG_FINISHED_APP_ERROR_MSG, e)
