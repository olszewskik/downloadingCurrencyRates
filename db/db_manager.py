from datetime import date
from db.engine import DbEngine
from db.session_manager import SessionManager
from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy import func
from db.models import ExchangeRateDaily, ExchangeRateMonthly, ExchangeRateCumulative

class DatabaseManager:
    def __init__(self, db_engine: DbEngine):
        self.session_manager = SessionManager(db_engine)

    def get_last_daily_rate_date(self) -> date:
        with self.session_manager.session_scope() as session:
            result = session.query(func.max(ExchangeRateDaily.date)).scalar()
            return result


    def insert_daily_rates(self, df: pd.DataFrame):
        if df.empty:
            print("🔕 Brak danych do zapisania (daily).")
            return

        with self.session_manager.session_scope() as session:
            for _, row in df.iterrows():
                obj = ExchangeRateDaily(
                    date=row["date"],
                    currency_code=row["currency_code"],
                    currency_name=row.get("currency_name", ""),
                    avg_rate=row.get("avg_rate"),
                )

                session.merge(obj)  # wykonuje UPSERT wg PK (date, currency_code)

        print(f"✅ Zapisano {len(df)} kursów dziennych do bazy.")


    def insert_monthly_rates(self, df: pd.DataFrame):
        if df.empty:
            print("🔕 Brak danych do zapisania (monthly).")
            return

        with self.session_manager.session_scope() as session:
            for _, row in df.iterrows():
                obj = ExchangeRateMonthly(
                    year_month_key=row["year_month_key"],
                    year=row["year"],
                    month=row["month"],
                    currency_code=row["currency_code"],
                    currency_name=row.get("currency_name", ""),
                    avg_monthly_rate=row.get("rate"),
                )

                session.merge(obj)

        print(f"✅ Zapisano {len(df)} kursów miesięcznych do bazy.")


    def insert_cumulative_rates(self, df: pd.DataFrame):
        if df.empty:
            print("🔕 Brak danych do zapisania (cumulative).")
            return

        with self.session_manager.session_scope() as session:
            for _, row in df.iterrows():
                obj = ExchangeRateCumulative(
                    year_month_key=row["year_month_key"],
                    year=row["year"],
                    month=row["month"],
                    currency_code=row["currency_code"],
                    currency_name=row.get("currency_name", ""),
                    avg_cumulative_rate=row.get("rate"),
                )

                session.merge(obj)

        print(f"✅ Zapisano {len(df)} kursów narastających do bazy.")