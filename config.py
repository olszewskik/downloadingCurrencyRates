import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Wczytaj dane z pliku .env
load_dotenv()

# Odczyt zmiennych środowiskowych
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# Tworzenie URL-a dla SQLAlchemy
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# API NBP
NBP_API_URL_DAILY_RATE_TEMPLATE = "https://api.nbp.pl/api/exchangerates/tables/A/{date}/?format=json"

