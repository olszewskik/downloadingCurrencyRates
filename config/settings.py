import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

class Config:
    """
    Klasa Config odpowiada za konfigurację aplikacji, w tym ustawienia bazy danych oraz API NBP.
    Wartości są pobierane z pliku `.env` lub zmiennych środowiskowych.
    """

    def __init__(self):
        """
        Inicjalizuje obiekt Config i ładuje konfigurację z pliku `.env`.
        Wywołuje metody konfigurujące bazę danych oraz API NBP.
        """
        load_dotenv()
        self.setup_logging()
        self.setup_database()
        self.setup_api_nbp()
        self.setup_csv_nbp()

    def setup_database(self):
        """
        Konfiguruje ustawienia bazy danych na podstawie zmiennych środowiskowych.
        Obsługiwane silniki baz danych: PostgreSQL, MSSQL, MySQL.

        :raises ValueError: Jeśli podano nieobsługiwany silnik bazy danych.
        """
        self.DB_ENGINE = os.getenv("DB_ENGINE", "postgresql").lower()
        self.DB_USER = os.getenv("DB_USER")
        self.DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
        self.DB_HOST = os.getenv("DB_HOST")
        self.DB_PORT = os.getenv("DB_PORT")
        self.DB_NAME = os.getenv("DB_NAME")

        # Domyślne porty dla obsługiwanych silników baz danych
        if not self.DB_PORT:
            self.DB_PORT = {
                "postgresql": "5432",
                "mssql": "1433",
                "mysql": "3306"
            }.get(self.DB_ENGINE, "")

        # Budowanie URL-a połączenia w zależności od silnika bazy danych
        if self.DB_ENGINE == "postgresql":
            self.DB_URL = f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        elif self.DB_ENGINE == "mssql":
            self.DB_URL = f"mssql+pyodbc://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
        elif self.DB_ENGINE == "mysql":
            self.DB_URL = f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        else:
            raise ValueError(f"Nieobsługiwany silnik bazy danych: {self.DB_ENGINE}")

    def setup_api_nbp(self):
        """
        Konfiguruje podstawowy URL API NBP na podstawie zmiennych środowiskowych.
        """
        self.NBP_API_BASE_URL = os.getenv("NBP_API_BASE_URL", "https://api.nbp.pl/api/exchangerates/tables/A")

    def setup_csv_nbp(self):
        """
        Konfiguruje URL-e do plików CSV NBP na podstawie zmiennych środowiskowych.
        """
        self.NBP_CSV_BASE_URL = "https://static.nbp.pl/dane/kursy/Archiwum/"

    def setup_logging(self):
        """Konfiguruje ustawienia logowania."""
        # Ścieżki do plików log
        self.LOG_DIRECTORY = os.path.join(os.path.dirname(__file__), "..", "logs")
        self.LOG_FILE = os.path.join(self.LOG_DIRECTORY, "app.log")
        # self.ERROR_LOG_DIRECTORY = os.path.join("logs", "error")
        # self.DATA_LOG_DIRECTORY = os.path.join("logs", "data")

        # # Komunikaty do loggera
        # self.LOG_DB_CONNECT_SUCCESS_MSG = "The connection to the {db_system} database in the {environment} environment is correct."
        # self.LOG_DB_CONNECT_ERROR_MSG = "There was a problem connecting to the {db_system} database in the {environment} environment: {error}"
        self.LOG_STARTING_APP_MSG = "Starting the application ..."
        self.LOG_FINISHED_APP_SUCCESS_MSG = "Application finished successfully."
        self.LOG_FINISHED_APP_ERROR_MSG = ("Application terminated with an error: {error}")

        self.LOG_NO_DATA_FOUND_MSG = ("{method_name}: No data found for the given date range.")

        # self.LOG_EXTRACTION_STARTED_MSG = "{dataset_name} extraction process started."
        # self.LOG_EXTRACTION_FAILED_MSG = "{dataset_name} extraction failed: {error}"
        #
        # self.LOG_TRANSFORM_STARTED_MSG = "{dataset_name} transform process started."
        # self.LOG_TRANSFORM_FAILED_MSG = "{dataset_name} transform failed: {error}"
        #
        # self.LOG_LOAD_STARTED_MSG = "{dataset_name} load process started."
        # self.LOG_LOAD_FAILED_MSG = "{dataset_name} load failed: {error}"
        # self.LOG_WHSE_TRANSFER_ERROR = "{errors_quantity} error {dataset_name} records."
        # self.LOG_FAIL_LOAD_RECORD_MSG = (
        #     "Failed to load record at index {index}: {error}"
        # )
        # self.LOG_NEW_RECORDS_INSERT_MSG = (
        #     "{new_records_count} new {dataset_name} records"
        # )
        # self.LOG_COMMIT_FAILE_MSG = (
        #     "Failed to commit the records to the database: {error}"
        # )
        #
        # self.LOG_ETL_PROCESS_SUCCESS_MSG = (
        #     "{dataset_name} ETL process for completed successfully."
        # )
        # self.LOG_ERROR_SAVE_TO_FILE = "Error writing data to file: {error}"

# Globalna instancja Config
config = Config()