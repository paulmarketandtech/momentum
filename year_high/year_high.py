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
from utils import previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# pd.set_option("display.float_format", lambda x: f"{x:.0f}" if isinstance(x, (int, float)) else x)
# dat = yf.Ticker("LTM")

Base = declarative_base()
ABOVE_GIVEN_MC = 1_000_000_000

engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod


class YearHigh(Base):
    __tablename__ = "52week_highs"

    id = Column(Integer, primary_key=True)
    date_52week_high = Column(Date, nullable=True)
    ticker = Column(String, nullable=False, index=True)
    long_name = Column(String, nullable=True)
    market_cap = Column(Float, nullable=False)
    fifty_two_week_high = Column(Float, nullable=False)
    full_exchange_name = Column(String, nullable=False)
    fifty_two_week_range = Column(String, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class YearLow(Base):
    __tablename__ = "52week_lows"

    id = Column(Integer, primary_key=True)
    date_52week_low = Column(Date, nullable=True)
    ticker = Column(String, nullable=False, index=True)
    long_name = Column(String, nullable=True)
    market_cap = Column(Float, nullable=False)
    fifty_two_week_low = Column(Float, nullable=False)
    full_exchange_name = Column(String, nullable=False)
    fifty_two_week_range = Column(String, nullable=False)

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

REQUIRED_FIELDS = ["fiftyTwoWeekHigh", "fiftyTwoWeekLow"]
OPTIONAL_FIELDS = ["marketCap", "longName", "fiftyTwoWeekRange", "fullExchangeName"]


# readd filter after initial run
# .filter(AllTickersMonthlyUpdate.market_cap > ABOVE_GIVEN_MC)
def creating_list_of_all_tickers():
    list_of_tickers = [t.ticker for t in session.query(AllTickersMonthlyUpdate).all()]
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def fetch_stock_data(symbol_list: list[str], output_path: str) -> pd.DataFrame:
    rows = []
    all_fields = REQUIRED_FIELDS + OPTIONAL_FIELDS

    start = datetime.now()
    for i, ticker in enumerate(symbol_list):
        if (i + 1) % 500 == 0:
            logging.info(f"Processing {i + 1}/{len(symbol_list)}")
            print(f"Processing {i + 1}/{len(symbol_list)}")

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
            print(ticker)

        except Exception as e:
            logging.error(
                f"Error {ticker} while downloading from YF: {e}", exc_info=True
            )
            print(f"Error {ticker} while downloading from YF: {e}")
            continue

    df = pd.DataFrame(rows)

    df.to_csv(output_path, index=False)
    logging.info(f"Saved {len(df)} tickers to {output_path}")

    end = datetime.now()
    print(f"total time: {end-start}")
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

            print(f"Inserted {ticker} with high {new_high} on {today}")
        else:
            current_high = record.fifty_two_week_high
            if current_high is None or new_high > current_high:
                record.fifty_two_week_high = new_high
                record.date_52week_high = previous_day
                record.market_cap = marketCap

                print(
                    f"Updated {ticker}: high {current_high} → {new_high} on {previous_day}"
                )
            else:
                print(
                    f"{ticker}: not a new high (current {current_high} >= {new_high})"
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

            print(f"Inserted {ticker} with high {new_low} on {previous_day}")
        else:
            current_high = record.fifty_two_week_low
            if current_low is None or new_low < current_low:
                record.fifty_two_week_low = new_low
                record.date_52week_low = previous_day
                record.market_cap = marketCap

                print(
                    f"Updated {ticker}: low {current_low} → {new_low} on {previous_day}"
                )
            else:
                print(f"{ticker}: not a new low (current {current_low} >= {new_low})")

    session.commit()


# symbol_list = ["AAPL", "NVDA", "AMKR", "AMZN"]
symbol_list = creating_list_of_all_tickers()
print(len(symbol_list))

"""
GOOD
df = fetch_stock_data(
    symbol_list=symbol_list[10:18], output_path="all_tickers_high_low.csv"
)
"""
# print(df.head())
"""
df_high = df[["ticker", "fiftyTwoWeekHigh", "marketCap"]]
df_low = df[["ticker", "fiftyTwoWeekLow", "marketCap"]]
check_new_high(df_high)
check_new_low(df_low)
print(df_high)
print(20 * "-")
print(df_low)
"""
session.close()
"""
TODO:
add logs
functions should be more less working 
download DB, and run it 
"""


"""
NEW WORKFLOW 
get all tickers from AllTickers.. table 
download for all of them 52 week highs and lows and save it all to 2 tables 52week_high & low with current date 

Take all tickers with MC > $1B and daily itterate through all of them and download current highs/lows 
keep it in a DF 
compare each one of them with value in DB.
if a new higher high or lower low then update values and change the date to current 
else do nothing.

Filter DB for current date. if anything matches then there're new highs/low 
HOW TO KNOW IF IT'S A NEW HIGH OR LOW IN THIS CASE?!?!?!
two tables? 
"""
