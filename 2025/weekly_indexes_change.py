import logging
import os
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import and_

from utils import list_of_commodities, list_of_etfs, list_of_indexes

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting indexes weekly change populating")

Base = declarative_base()

print("Starting indexes weekly DB populating")


class SourceData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}')>"


class IndexesWeeklyChange(Base):
    __tablename__ = "indexes_weekly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_week_pct_change = Column(Float, nullable=False)
    four_week_pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class CommoditiesWeeklyChange(Base):
    __tablename__ = "commodities_weekly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_week_pct_change = Column(Float, nullable=False)
    four_week_pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class EtfsWeeklyChange(Base):
    __tablename__ = "etfs_weekly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_week_pct_change = Column(Float, nullable=False)
    four_week_pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# engine = create_engine(os.getenv("DB_STOCK_DATA"))
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def weekly_index_change(tickers, last_friday, four_weeks_ago_friday):
    for ticker in tickers:
        try:
            last_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == last_friday,
                )
                .first()
            )

            four_weeks_before_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == four_weeks_ago_friday,
                )
                .first()
            )

            four_weeks_returns = (
                (last_friday_data.close - four_weeks_before_friday_data.close)
                / four_weeks_before_friday_data.close
            ) * 100

            session.query(IndexesWeeklyChange).filter_by(
                ticker=ticker, date=last_friday
            ).update({"four_week_pct_change": four_weeks_returns})

            session.commit()

        except AttributeError:
            print("Bad ticker:", ticker)
            logging.error(
                f"Bad ticker: {ticker} in counting 4 weeks IndexesWeeklyChange change"
            )
    logging.info(f"Finished 4 weeks IndexesWeeklyChange change populating")


def weekly_commodity_change(tickers, last_friday, four_weeks_ago_friday):
    for ticker in tickers:
        try:
            last_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == last_friday,
                )
                .first()
            )

            four_weeks_before_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == four_weeks_ago_friday,
                )
                .first()
            )

            four_weeks_returns = (
                (last_friday_data.close - four_weeks_before_friday_data.close)
                / four_weeks_before_friday_data.close
            ) * 100

            session.query(CommoditiesWeeklyChange).filter_by(
                ticker=ticker, date=last_friday
            ).update({"four_week_pct_change": four_weeks_returns})

            session.commit()

        except AttributeError:
            print("Bad ticker:", ticker)
            logging.error(
                f"Bad ticker: {ticker} in counting 4 weeks CommoditiesWeeklyChange change"
            )
    logging.info(f"Finished 4 weeks CommoditiesWeeklyChange change populating")


def weekly_etfs_change(tickers, last_friday, four_weeks_ago_friday):
    for ticker in tickers:
        try:
            last_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == last_friday,
                )
                .first()
            )

            four_weeks_before_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == four_weeks_ago_friday,
                )
                .first()
            )

            four_weeks_returns = (
                (last_friday_data.close - four_weeks_before_friday_data.close)
                / four_weeks_before_friday_data.close
            ) * 100

            session.query(EtfsWeeklyChange).filter_by(
                ticker=ticker, date=last_friday
            ).update({"four_week_pct_change": four_weeks_returns})

            session.commit()

        except AttributeError:
            print("Bad ticker:", ticker)
            logging.error(
                f"Bad ticker: {ticker} in counting 4 weeks EtfsWeeklyChange change"
            )
    logging.info(f"Finished 4 weeks CommoditiesWeeklyChange change populating")


last_friday = date.today() - timedelta(days=1)
previous_friday = date.today() - timedelta(days=8)
four_weeks_ago_friday = date.today() - timedelta(days=29)

# -----INDEXES----------
query_indexes = session.query(
    SourceData.date, SourceData.ticker, SourceData.weekly_change
).filter(and_(SourceData.ticker.in_(list_of_indexes), SourceData.date == last_friday))

results_indexes = query_indexes.all()

df_indexes = pd.DataFrame(results_indexes, columns=["Date", "Ticker", "Weekly_change"])
df_indexes["Date"] = pd.to_datetime(df_indexes["Date"]).dt.date
df_indexes = df_indexes.dropna()

df_weekly_indexes_sorted = df_indexes.sort_values(by="Weekly_change", ascending=False)

for _, row in df_weekly_indexes_sorted.iterrows():
    stock_data = IndexesWeeklyChange(
        date=row["Date"],
        ticker=row["Ticker"],
        one_week_pct_change=row["Weekly_change"],
    )
    session.add(stock_data)


# ------COMMODITIES----------
query_commodities = session.query(
    SourceData.date, SourceData.ticker, SourceData.weekly_change
).filter(
    and_(SourceData.ticker.in_(list_of_commodities), SourceData.date == last_friday)
)

results_commodities = query_commodities.all()
for r in results_commodities:
    print(r.date, r.ticker, r.weekly_change)

df_commodities = pd.DataFrame(
    results_commodities, columns=["Date", "Ticker", "Weekly_change"]
)
df_commodities["Date"] = pd.to_datetime(df_commodities["Date"]).dt.date
df_commodities = df_commodities.dropna()

df_weekly_commodities_sorted = df_commodities.sort_values(
    by="Weekly_change", ascending=False
)

for _, row in df_weekly_commodities_sorted.iterrows():
    stock_data = CommoditiesWeeklyChange(
        date=row["Date"],
        ticker=row["Ticker"],
        one_week_pct_change=row["Weekly_change"],
    )
    session.add(stock_data)


# -----ETFs----------
query_etfs = session.query(
    SourceData.date, SourceData.ticker, SourceData.weekly_change
).filter(and_(SourceData.ticker.in_(list_of_etfs), SourceData.date == last_friday))

results_etfs = query_etfs.all()

df_etfs = pd.DataFrame(results_etfs, columns=["Date", "Ticker", "Weekly_change"])
df_etfs["Date"] = pd.to_datetime(df_etfs["Date"]).dt.date
df_etfs = df_etfs.dropna()

df_weekly_etfs_sorted = df_etfs.sort_values(by="Weekly_change", ascending=False)

for _, row in df_weekly_etfs_sorted.iterrows():
    stock_data = EtfsWeeklyChange(
        date=row["Date"],
        ticker=row["Ticker"],
        one_week_pct_change=row["Weekly_change"],
    )
    session.add(stock_data)

session.commit()

weekly_index_change(list_of_indexes, last_friday, four_weeks_ago_friday)
weekly_commodity_change(list_of_commodities, last_friday, four_weeks_ago_friday)
weekly_etfs_change(list_of_etfs, last_friday, four_weeks_ago_friday)

session.close()

logging.info(f"Finished indexes weekly DB populating")

import runpy
import time

logging.info("5 seconds sleep after indexes weekly is done")
time.sleep(5)
try:
    runpy.run_path(path_name=os.getenv("TG_BOT_PATH"))
except Exception as e:
    logging.error(f"Error in running tg_bot.py: {e}")
