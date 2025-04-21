import pandas as pd
import requests
from io import StringIO

class CSVRateLoader:
    """
    Klasa do pobierania i czyszczenia danych kursów średnioważonych miesięcznych i narastających z plików CSV NBP.
    """

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def load_csv(self, year: int, rate_type: str) -> pd.DataFrame:
        """
        Pobiera i czyści dane z pliku CSV z serwera NBP.

        rate_type: 'monthly' lub 'cumulative'
        """
        suffix = {
            'monthly': 'publ_sredni_m',
            'cumulative': 'publ_sredni_n',
        }.get(rate_type)

        if not suffix:
            raise ValueError("Nieobsługiwany typ kursu: " + rate_type)

        url = f"{self.base_url}/{suffix}_{year}.csv"
        print(f"⬇️ Pobieranie pliku: {url}")

        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.content.decode('cp1250')), sep=';', skiprows=1)
        df = df.loc[:, ~df.columns.str.contains('Unnamed')]

        base_cols = ['currency_name', 'currency_code', 'multiplier']
        monthly_cols = [f'm{i + 1}' for i in range(len(df.columns) - 3)]
        df.columns = base_cols + monthly_cols[:12]  # ogranicz do 12 miesiecy

        df_long = df.melt(id_vars=base_cols, var_name='month_index', value_name='rate')
        df_long['rate'] = pd.to_numeric(df_long['rate'].str.replace(',', '.'), errors='coerce')
        df_long['month'] = df_long['month_index'].str.extract(r'(\d+)').astype(int)
        df_long['year'] = year
        df_long['year_month_key'] = (df_long['year'] * 100) + df_long['month']

        return df_long[['year_month_key', 'year', 'month', 'currency_code', 'currency_name', 'rate']].dropna(subset=['rate'])