from app.clients.nbp_api_client import NBPApiClient
from app.loaders.csv_loader import CSVRateLoader
from app.services.exchange_rate_saver import ExchangeRateSaver
from datetime import date, timedelta
import pandas as pd


class ExchangeRateManager:
    """
    Orkiestrator logiki synchronizacji kursów z różnych źródeł do bazy danych.
    """

    def __init__(self, api_client: NBPApiClient, csv_loader: CSVRateLoader, saver: ExchangeRateSaver):
        self.api_client = api_client
        self.csv_loader = csv_loader
        self.saver = saver

    def sync_daily_rates(self, start_date: str, end_date: str):
        """
        Pobiera dzienne kursy z API NBP i zapisuje je do bazy danych.
        """
        df = self.api_client.get_rates_for_dates(start_date, end_date)
        self.saver.save_daily_rates(df)


    def sync_daily_rates_auto(self):
        """
        Pobiera i zapisuje brakujące kursy dzienne – od ostatniego dnia w bazie do dzisiaj.
        """
        last_date = self.saver.db_manager.get_last_daily_rate_date()
        if last_date is None:
            print("📭 Brak danych w bazie – wymagane podanie daty początkowej.")
            return

        start_date = last_date + timedelta(days=1)
        end_date = date.today()

        if start_date > end_date:
            print("✅ Dane dzienne są aktualne – nic do pobrania.")
            return

        self.sync_daily_rates(start_date.isoformat(), end_date.isoformat())

    def sync_monthly_rates(self, year: int):
        """
        Pobiera kursy średnioważone miesięczne z CSV NBP i zapisuje je do bazy danych.
        """
        df = self.csv_loader.load_csv(year, rate_type="monthly")
        self.saver.save_weighted_rates(df, rate_type="monthly")

    def sync_cumulative_rates(self, year: int):
        """
        Pobiera kursy średnioważone narastająco z CSV NBP i zapisuje je do bazy danych.
        """
        df = self.csv_loader.load_csv(year, rate_type="cumulative")
        self.saver.save_weighted_rates(df, rate_type="cumulative")

    def sync_all(self, year: int):
        """
        Uruchamia wszystkie procesy synchronizacji: dzienne, miesięczne, narastające.
        """
        self.sync_daily_rates_auto()
        self.sync_monthly_rates(year)
        self.sync_cumulative_rates(year)
