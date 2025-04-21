import pandas as pd
from db.db_manager import DatabaseManager

class ExchangeRateSaver:
    """
    Odpowiada za zapis danych o kursach walut do odpowiednich tabel.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def save_daily_rates(self, df: pd.DataFrame):
        """
        Zapisuje kursy dzienne do tabeli exchange_rate_daily.
        """
        self.db_manager.insert_daily_rates(df)

    def save_weighted_rates(self, df: pd.DataFrame, rate_type: str):
        """
        Zapisuje kursy średnioważone miesięczne lub narastające do odpowiedniej tabeli.

        rate_type: 'monthly' = miesięczne, 'cumulative' = narastające
        """
        if rate_type == "monthly":
            self.db_manager.insert_monthly_rates(df)
        elif rate_type == "cumulative":
            self.db_manager.insert_cumulative_rates(df)
        else:
            raise ValueError("Nieobsługiwany typ kursu: " + rate_type)