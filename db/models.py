from sqlalchemy import Column, Date, String, Numeric, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class ExchangeRateDaily(Base):
    __tablename__ = "exchange_rate_daily"

    date = Column(Date, primary_key=True)
    currency_code = Column(String(3), primary_key=True)
    currency_name = Column(String)
    avg_rate = Column(Numeric(12, 6))
    load_date = Column(DateTime, default=datetime.utcnow)


class ExchangeRateMonthly(Base):
    __tablename__ = "exchange_rate_monthly"

    year_month_key = Column(Integer, primary_key=True)  # YYYYMM
    year = Column(Integer)
    month = Column(Integer)
    currency_code = Column(String(3), primary_key=True)
    currency_name = Column(String)
    avg_monthly_rate = Column(Numeric(12, 6))
    load_date = Column(DateTime, default=datetime.utcnow)


class ExchangeRateCumulative(Base):
    __tablename__ = "exchange_rate_cumulative"

    year_month_key = Column(Integer, primary_key=True)  # YYYYMM
    year = Column(Integer)
    month = Column(Integer)
    currency_code = Column(String(3), primary_key=True)
    currency_name = Column(String)
    avg_cumulative_rate = Column(Numeric(12, 6))
    load_date = Column(DateTime, default=datetime.utcnow)

