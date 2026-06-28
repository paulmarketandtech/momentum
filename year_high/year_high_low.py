import sys
from pathlib import Path

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(parent_dir))
import logging
import os
from datetime import date, datetime, timedelta
from pprint import pprint

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from src.utils import previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# pd.set_option("display.float_format", lambda x: f"{x:.0f}" if isinstance(x, (int, float)) else x)

Base = declarative_base()
ABOVE_GIVEN_MC = 1_000_000_000
REQUIRED_FIELDS = ["fiftyTwoWeekHigh", "fiftyTwoWeekLow"]
OPTIONAL_FIELDS = ["marketCap", "longName", "fiftyTwoWeekRange", "fullExchangeName"]

engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod


class YearHigh(Base):
    __tablename__ = "52week_highs"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    fifty_two_week_high = Column(Float, nullable=False)
    date_52week_high = Column(Date, nullable=False)
    long_name = Column(String)
    market_cap = Column(Float)
    full_exchange_name = Column(String)
    fifty_two_week_range = Column(String)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class YearLow(Base):
    __tablename__ = "52week_lows"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    fifty_two_week_low = Column(Float, nullable=False)
    date_52week_low = Column(Date, nullable=False)
    long_name = Column(String)
    market_cap = Column(Float)
    full_exchange_name = Column(String)
    fifty_two_week_range = Column(String)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class AllTickersMonthlyUpdate(Base):
    __tablename__ = "all_tickers_monthly_update"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Integer, nullable=False)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', MC={self.market_cap})>"


# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def creating_list_of_all_tickers(above_given_MC):
    list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.market_cap > above_given_MC)
        .all()
    ]

    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def fetch_stock_data(symbol_list: list[str]) -> pd.DataFrame:
    rows = []
    all_fields = REQUIRED_FIELDS + OPTIONAL_FIELDS

    start = datetime.now()
    for i, ticker in enumerate(symbol_list):
        if (i + 1) % 500 == 0:
            logging.info(f"Processing {i + 1}/{len(symbol_list)}")
            logging.info(datetime.now() - start)

        try:
            info = yf.Ticker(ticker).info

            missing_required = [
                field
                for field in REQUIRED_FIELDS
                if field not in info or info[field] is None
            ]

            if missing_required:
                logging.warning(
                    f"{ticker}: missing required fields {missing_required}, skipping"
                )
                continue

            row = {"ticker": ticker}
            for field in all_fields:
                row[field] = info.get(field)

            rows.append(row)

        except Exception as e:
            logging.error(
                f"Error {ticker} while downloading from YF: {e}", exc_info=True
            )
            continue

    df = pd.DataFrame(rows)

    end = datetime.now()
    logging.info(f"total time: {end-start}")
    return df


def check_new_high(df_high):
    for _, row in df_high.iterrows():
        ticker = row["ticker"]
        new_high = row["fiftyTwoWeekHigh"]
        marketCap = row["marketCap"]
        longName = row["longName"]
        fiftyTwoWeekRange = row["fiftyTwoWeekRange"]
        fullExchangeName = row["fullExchangeName"]

        record = session.query(YearHigh).filter_by(ticker=ticker).first()

        if record is None:
            # New ticker – insert with current date
            session.add(
                YearHigh(
                    ticker=ticker,
                    fifty_two_week_high=new_high,
                    date_52week_high=previous_day,
                    market_cap=marketCap,
                    long_name=longName,
                    fifty_two_week_range=fiftyTwoWeekRange,
                    full_exchange_name=fullExchangeName,
                )
            )
            logging.info(
                f"NEW TICKER. Inserted {ticker} with high {new_high} on {previous_day}"
            )
        else:
            current_high = record.fifty_two_week_high
            if current_high is None or new_high > current_high:
                record.fifty_two_week_high = new_high
                record.date_52week_high = previous_day
                record.market_cap = marketCap

                logging.info(
                    f"NEW HIGH. Updated {ticker}: high {current_high} → {new_high} on {previous_day}"
                )
    session.commit()


def check_new_low(df_low):
    for _, row in df_low.iterrows():
        ticker = row["ticker"]
        new_low = row["fiftyTwoWeekLow"]
        marketCap = row["marketCap"]
        longName = row["longName"]
        fiftyTwoWeekRange = row["fiftyTwoWeekRange"]
        fullExchangeName = row["fullExchangeName"]

        # Get existing record (or None)
        record = session.query(YearLow).filter_by(ticker=ticker).first()

        if record is None:
            session.add(
                YearLow(
                    ticker=ticker,
                    fifty_two_week_low=new_low,
                    date_52week_low=previous_day,
                    market_cap=marketCap,
                    long_name=longName,
                    fifty_two_week_range=fiftyTwoWeekRange,
                    full_exchange_name=fullExchangeName,
                )
            )
            logging.info(
                f"NEW TICKER. Inserted {ticker} with low {new_low} on {previous_day}"
            )
        else:
            current_low = record.fifty_two_week_low
            if current_low is None or new_low < current_low:
                record.fifty_two_week_low = new_low
                record.date_52week_low = previous_day
                record.market_cap = marketCap

                logging.info(
                    f"NEW LOW. Updated {ticker}: low {current_low} → {new_low} on {previous_day}"
                )
    session.commit()


def main():
    symbol_list = creating_list_of_all_tickers(ABOVE_GIVEN_MC)

    df = fetch_stock_data(symbol_list=symbol_list)

    df_high = df[
        [
            "ticker",
            "fiftyTwoWeekHigh",
            "marketCap",
            "longName",
            "fiftyTwoWeekRange",
            "fullExchangeName",
        ]
    ]
    df_low = df[
        [
            "ticker",
            "fiftyTwoWeekLow",
            "marketCap",
            "longName",
            "fiftyTwoWeekRange",
            "fullExchangeName",
        ]
    ]
    check_new_high(df_high)
    check_new_low(df_low)

    session.close()


if __name__ == "__main__":
    main()
