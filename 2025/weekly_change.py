from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy import Float, Date, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import logging, os
from dotenv import load_dotenv
from utils import previous_day, list_of_tickers_5B, list_of_indexes, list_of_commodities

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting weekly change populating")

Base = declarative_base()

print("Starting best/worst weekly DB populating")


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class SourceData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}')>"


class Weekly20Best(Base):
    __tablename__ = "weekly_change_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


class Weekly20Worst(Base):
    __tablename__ = "weekly_change_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def creating_list_of_tickers(list_of_tickers, list_of_indexes, list_of_commodities):
    list_of_tickers.extend(list_of_indexes)
    list_of_tickers.extend(list_of_commodities)
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def weekly_change(tickers, previous_day, last_friday):
    for ticker in tickers:
        try:
            previous_day_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == previous_day,
                )
                .first()
            )
            last_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == last_friday,
                )
                .first()
            )

            weekly_returns = (
                (previous_day_data.close - last_friday_data.close)
                / last_friday_data.close
            ) * 100

            session.query(SourceData).filter_by(
                ticker=ticker, date=previous_day
            ).update({"weekly_change": weekly_returns})

            session.commit()

        except AttributeError:
            print("Bad ticker:", ticker)
            logging.error(f"Bad ticker: {ticker} in counting weekly change")
    logging.info(f"Finished weekly change populating")


days_shift = {
    "tuesday": 4,
    "wednesday": 5,
    "thursday": 6,
    "friday": 7,
    "saturday": 8,
}
today = datetime.today().strftime("%A")
last_friday = date.today() - timedelta(days=days_shift[today.lower()])

list_of_tickers = creating_list_of_tickers(
    list_of_tickers_5B, list_of_indexes, list_of_commodities
)
weekly_change(list_of_tickers, previous_day, last_friday)

query_result = (
    session.query(SourceData.date, SourceData.ticker, SourceData.weekly_change)
    .filter(SourceData.date == previous_day)
    .all()
)

df = pd.DataFrame(query_result, columns=["Date", "Ticker", "Weekly_change"])
df["Date"] = pd.to_datetime(df["Date"]).dt.date
df = df.dropna()

df_weekly_sorted = df.sort_values(by="Weekly_change", ascending=False)

weekly_top20 = df_weekly_sorted.head(50)
weekly_worst20 = df_weekly_sorted.tail(50)
weekly_worst20 = weekly_worst20.sort_values(by="Weekly_change", ascending=True)

for _, row in weekly_top20.iterrows():
    stock_data = Weekly20Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Weekly_change"],
    )
    session.add(stock_data)

for _, row in weekly_worst20.iterrows():
    stock_data = Weekly20Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Weekly_change"],
    )
    session.add(stock_data)

session.commit()

session.close()

logging.info(f"Finished best/worst weekly DB populating")

import time
import runpy

print("5 seconds sleep after weekly is done")
time.sleep(5)
today = datetime.today().strftime("%A")
try:
    if today.lower() == "saturday":
        runpy.run_path(path_name=os.getenv("WEEKLY_INDEXES_CHANGE_PATH"))
    else:
        runpy.run_path(path_name=os.getenv("TG_BOT_PATH"))
except Exception as e:
    logging.error(f"Error in running tg_bot.py: {e}")
