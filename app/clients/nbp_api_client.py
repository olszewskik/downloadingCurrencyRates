import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


class NBPApiClient:
    """
    Klient do pobierania dziennych kursów walut z API NBP (Tabela A).
    """

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def get_rates_by_date(self, date_str: str) -> pd.DataFrame:
        """
        Pobiera dzienne kursy walut z API NBP dla konkretnej daty.

        Parametry:
            date_str (str): Data w formacie 'YYYY-MM-DD'

        Zwraca:
            pd.DataFrame: Kolumny: date, currency_code, currency_name, avg_rate.
                          Jeśli brak danych (np. dzień wolny), zwraca pusty DataFrame.
        """
        url = f"{self.base_url}/{date_str}/?format=json"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            rates = data[0].get("rates", [])
            effective_date = data[0].get("effectiveDate")

            if not rates or not effective_date:
                print(f"⚠️ Brak danych dla {date_str}")
                return pd.DataFrame()

            df = pd.DataFrame(rates)
            df["date"] = pd.to_datetime(effective_date)
            df["avg_rate"] = df["mid"]
            df = df.rename(columns={"code": "currency_code", "currency": "currency_name"})

            return df[["date", "currency_code", "currency_name", "avg_rate"]]

        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                return pd.DataFrame()
            print(f"❌ Błąd HTTP przy pobieraniu kursów NBP dla {date_str}: {e}")
            return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            print(f"❌ Błąd połączenia z API NBP dla {date_str}: {e}")
            return pd.DataFrame()

        except Exception as e:
            print(f"❌ Nieoczekiwany błąd przy pobieraniu kursów NBP dla {date_str}: {e}")
            return pd.DataFrame()

    def get_rates_for_dates(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Pobiera dzienne kursy dla zakresu dat. Pomija dni wolne.

        Parametry:
            start_date (str): 'YYYY-MM-DD'
            end_date (str): 'YYYY-MM-DD'

        Zwraca:
            pd.DataFrame: Zbiorczy DataFrame z wielu dni
        """

        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        all_data = []
        current = start

        while current <= end:
            date_str = current.isoformat()
            df = self.get_rates_by_date(date_str)
            if not df.empty:
                all_data.append(df)
            current += timedelta(days=1)

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
