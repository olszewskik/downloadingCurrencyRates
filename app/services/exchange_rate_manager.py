from app.clients.nbp_api_client import NBPApiClient
from app.loaders.csv_loader import CSVRateLoader
from app.services.exchange_rate_saver import ExchangeRateSaver
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

    def sync_all(self, start_date: str, end_date: str, year: int):
        """
        Uruchamia wszystkie procesy synchronizacji: dzienne, miesięczne, narastające.
        """
        self.sync_daily_rates(start_date, end_date)
        self.sync_monthly_rates(year)
        self.sync_cumulative_rates(year)
