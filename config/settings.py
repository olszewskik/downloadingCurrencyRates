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
        self.NBP_CSV_MONTHLY_URL = os.getenv("NBP_CSV_MONTHLY_URL", "https://static.nbp.pl/dane/kursy/Archiwum/publ_sredni_m_2025.csv")
        self.NBP_CSV_CUMULATIVE_URL = os.getenv("NBP_CSV_CUMULATIVE_URL", "https://static.nbp.pl/dane/kursy/Archiwum/publ_sredni_n_2025.csv")


# Globalna instancja Config
config = Config()