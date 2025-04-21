from app.application import Application
from datetime import date

if __name__ == "__main__":
    app = Application()

    # Przykładowe dane wejściowe
    start_date = "2025-04-01"
    end_date = date.today().isoformat()
    year = 2025

    # Pełna synchronizacja
    app.run_sync(start_date=start_date, end_date=end_date, year=year)

    # Można też uruchomić tylko wybrane fragmenty:
    # app.run_daily_only(start_date, end_date)
    # app.run_monthly_only(year)
    # app.run_cumulative_only(year)
