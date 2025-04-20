import pandas as pd
import requests
from io import StringIO
from sqlalchemy import text
from datetime import datetime
from db import get_engine

def download_nbp_csv(year: int, rate_type: str) -> pd.DataFrame:
    """
    Pobiera plik CSV z serwera NBP dla wybranego typu kursu i roku.

    Parametry:
        year (int): Rok, np. 2024
        rate_type (str): Typ kursu ('average' - Kurs średni, 'average_monthly' - Kurs średnioważony miesięczny, 'average_cumulative' - Kurs średnioważony narastająco)

    Zwraca:
        pd.DataFrame: Dane z pliku CSV
    """
    base_url = "https://static.nbp.pl/dane/kursy/Archiwum/"

    rate_map = {
        'average': f"archiwum_tab_a_{year}.csv",
        'average_monthly': f"publ_sredni_m_{year}.csv",
        'average_cumulative': f"publ_sredni_n_{year}.csv"
    }

    if rate_type not in rate_map:
        raise ValueError(f"Nieprawidłowy typ kursu: {rate_type}. Wybierz z: {list(rate_map.keys())}")

    url = base_url + rate_map[rate_type]
    print(f"Pobieranie danych z: {url}")

    response = requests.get(url)
    response.raise_for_status()

    content = response.content.decode('iso-8859-2')
    df = pd.read_csv(StringIO(content), sep=';', header=0)

    # Usuwanie pustej kolumny z końca (jeśli występuje)
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    return df

def clean_nbp_data(df: pd.DataFrame, kurs_type: str, year: int) -> pd.DataFrame:
    """
    Czyści dane NBP z pliku CSV w zależności od typu kursu.

    Parametry:
        df (pd.DataFrame): Dane wczytane z CSV
        kurs_type (str): Typ kursu ('sredni', 'sredni_m', 'sredni_n')
        year (int): Rok, do którego dotyczą dane

    Zwraca:
        pd.DataFrame: Dane w formacie (date, currency_code, currency_name, multiplier, rate)
    """
    if kurs_type == 'sredni':
        df = df.reset_index()
        df = df.drop(index=0)


        df_long = pd.melt(df, id_vars=['data'], var_name='currency', value_name='rate')
        print(df_long)
        df_long.to_excel('test.xlsx', index=False)

        return df_long


        # # Wyciąganie daty z pierwszych 8 znaków (np. '20240102')
        # df['date_raw'] = df.iloc[:, 0].astype(str).str[:8]
        # df['date'] = pd.to_datetime(df['date_raw'], format='%Y%m%d', errors='coerce')
        # df = df.drop(columns=[df.columns[0], 'date_raw'])
        #
        # # Usuwanie kolumn technicznych i numerów tabeli NBP
        # df = df.loc[:, ~df.columns.str.contains('Unnamed')]
        # df = df.loc[:, ~df.columns.str.contains('NBP', na=False)]
        #
        # # Przekształcenie do formatu długiego
        # df_long = df.melt(id_vars='date', var_name='currency_name', value_name='rate')
        # df_long['rate'] = pd.to_numeric(df_long['rate'].str.replace(',', '.'), errors='coerce')
        #
        # df_long['currency_code'] = df_long['currency_name'].str.extract(r'\((\w+)\)')
        # df_long['currency_name'] = df_long['currency_name'].str.replace(r'\s*\(\w+\)', '', regex=True)
        #
        # df_long['multiplier'] = 1
        #
        # return df_long[['date', 'currency_code', 'currency_name', 'multiplier', 'rate']].dropna(subset=['rate'])


    elif kurs_type in ('sredni_m', 'sredni_n'):
        # Pliki średnioważone: waluta, symbol, mnożnik, 12 miesięcy
        df = df.loc[:, ~df.columns.str.contains('Unnamed')]
        base_cols = ['currency_name', 'currency_code', 'multiplier']
        monthly_cols = [f'm{i + 1}' for i in range(len(df.columns) - 3)]
        df.columns = base_cols + monthly_cols
        monthly_cols = monthly_cols[:12]
        df = df[base_cols + monthly_cols]

        df_long = df.melt(id_vars=base_cols, var_name='month_index', value_name='rate')
        df_long['rate'] = pd.to_numeric(df_long['rate'].str.replace(',', '.'), errors='coerce')
        df_long['month'] = df_long['month_index'].str.extract(r'(\d+)').astype(int)
        df_long['date'] = pd.to_datetime(f"{year}-" + df_long['month'].astype(str) + "-01")

        return df_long[['date', 'currency_code', 'multiplier', 'rate']].dropna(subset=['rate'])

    else:
        raise ValueError(f"Nieobsługiwany typ kursu: {kurs_type}")

def save_to_db(df: pd.DataFrame, rate_type: str):
    """
    Zapisuje dane do tabeli exchange_rate w bazie danych.

    Parametry:
        df (pd.DataFrame): Dane po oczyszczeniu
        kurs_type (str): Typ kursu ('sredni', 'sredni_m', 'sredni_n')
    """
    if rate_type not in ['average', 'average_monthly', 'average_cumulative']:
        raise ValueError("Nieobsługiwany typ kursu")

    column_map = {
        'average': 'avg_rate',
        'average_monthly': 'avg_monthly_rate',
        'average_cumulative': 'avg_cumulative_rate'
    }

    target_column = column_map[rate_type]

    df_to_insert = df.copy()
    df_to_insert['load_date'] = datetime.now()
    df_to_insert = df_to_insert.rename(columns={
        'rate': target_column
    })

    # Pozostałe kolumny muszą istnieć, nawet jeśli są puste
    for col in ['avg_rate', 'avg_monthly_rate', 'avg_cumulative_rate']:
        if col not in df_to_insert.columns:
            df_to_insert[col] = None

    df_to_insert = df_to_insert[['date', 'currency_code', 'multiplier',
                                 'avg_rate', 'avg_monthly_rate', 'avg_cumulative_rate', 'load_date']]

    engine = get_engine()
    with engine.begin() as connection:
        for _, row in df_to_insert.iterrows():
            stmt = text(f"""
                INSERT INTO exchange_rate (date, currency_code, multiplier, avg_rate,
                                           avg_monthly_rate, avg_cumulative_rate, load_date)
                VALUES (:date, :currency_code, :multiplier, :avg_rate,
                        :avg_monthly_rate, :avg_cumulative_rate, :load_date)
                ON CONFLICT (date, currency_code) DO UPDATE SET
                    {target_column} = EXCLUDED.{target_column},
                    multiplier = EXCLUDED.multiplier,
                    load_date = EXCLUDED.load_date
            """)
            connection.execute(stmt, row.to_dict())

def test_connection():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Połączenie z bazą działa!")
            for row in result:
                print("Wynik testu:", row[0])
    except Exception as e:
        print("❌ Błąd połączenia:", e)

if __name__ == '__main__':
    year = 2025
    df_raw = download_nbp_csv(year=year, rate_type='average_monthly')
    df_clean = clean_nbp_data(df_raw, kurs_type='sredni_m', year=year)
    df_clean.to_excel('dane.xlsx', index=False)

    test_connection()

    save_to_db(df_clean, 'average_monthly')