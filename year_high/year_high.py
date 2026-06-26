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

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
pd.set_option(
    "display.float_format", lambda x: f"{x:.0f}" if isinstance(x, (int, float)) else x
)
# dat = yf.Ticker("LTM")
# print(dat.info["fiftyTwoWeekHigh"])
# pprint(dat.info)


Base = declarative_base()

# engine = create_engine(os.getenv("DB_STOCK_DATA")) # prod

# ==== DEV =====
engine_before = create_engine("sqlite:///historical_stock_data_20260622.db")
engine_after = create_engine("sqlite:///historical_stock_data_20260623.db")


class YearHighLow(Base):
    __tablename__ = "52week_highs_lows"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=True)
    ticker = Column(String, nullable=False, index=True)
    name = Column(String, nullable=True)
    marketCap = Column(Float, nullable=False)
    fiftyTwoWeekHigh = Column(Float, nullable=False)
    currentHigh = Column(Float, nullable=True)
    newHigh = Column(Boolean, nullable=True, default=False)
    fiftyTwoWeekLow = Column(Float, nullable=False)
    currentLow = Column(Float, nullable=True)
    newLow = Column(Boolean, nullable=True, default=False)
    fullExchangeName = Column(String, nullable=False)
    fiftyTwoWeekRange = Column(String, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=True)
    ticker = Column(String, nullable=False, index=True)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)


# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def create_tickers_df_MC_lt100M_from_file(filename):
    df = pd.read_csv(filename, usecols=["Symbol", "Name", "Market Cap"])
    df["Symbol"] = df["Symbol"].astype(str).str.strip()
    df["Name"] = df["Name"].astype(str).str.strip()

    df["Symbol"].replace(["", "nan", "NaN", "null", "None"], np.nan, inplace=True)
    df["Name"].replace(["", "nan", "NaN", "null", "None"], np.nan, inplace=True)
    df["Market Cap"] = pd.to_numeric(df["Market Cap"], errors="coerce")

    df = df.dropna(subset=["Symbol", "Name", "Market Cap"])

    df = df.reset_index(drop=True)
    df_hundred_million = df[df["Market Cap"] >= 100_000_000]
    df_MC_sorted = df_hundred_million.sort_values(by="Market Cap", ascending=False)
    df_MC_sorted.to_csv("more_then_hundred_million_ticker.csv", index=False)
    print(len(df_MC_sorted))
    print("-----------")
    return df_MC_sorted


REQUIRED_FIELDS = ["fiftyTwoWeekHigh", "fiftyTwoWeekLow"]
OPTIONAL_FIELDS = ["marketCap", "longName", "fiftyTwoWeekRange", "fullExchangeName"]


def get_52_week_high_from_YF(df_loaded):
    # df_loaded = pd.read_csv(filename)
    start = datetime.now()
    results = []
    symbol_list = df_loaded["Symbol"].tolist()

    for i, ticker in enumerate(symbol_list):
        i += 1
        if i % 500 == 0:
            print(i)
            checkpoint = datetime.now()
            print(f"time: {checkpoint - start}")

            """
            marketCap = dat["marketCap"]
            name = dat["longName"]
            fullExchangeName = dat["fullExchangeName"]
            fiftyTwoWeekHigh = dat["fiftyTwoWeekHigh"]
            fiftyTwoWeekLow = dat["fiftyTwoWeekLow"]
            fiftyTwoWeekRange = dat["fiftyTwoWeekRange"]
            # print(f"{ticker}: {year_high}")
            """

        try:
            dat = yf.Ticker(ticker).info
            if any(field not in info for field in REQUIRED_FIELDS):
                logging.warning(f"{ticker}: missing required fields, skipping")
                continue

            row = {"ticker": ticker}
            for field in REQUIRED_FIELDS + OPTIONAL_FIELDS:
                row[field] = info.get(field, None)

            rows.append(row)
        except Exception as e:
            print(f"Error {ticker}: {e}")
            logging.error(
                f"Error {ticker} while downloading from YF: {e}", exc_info=True
            )
            continue

        results.append(
            {
                "ticker": ticker,
                "marketCap": marketCap,
                "name": name,
                "fullExchangeName": fullExchangeName,
                "fiftyTwoWeekHigh": fiftyTwoWeekHigh,
                "fiftyTwoWeekLow": fiftyTwoWeekLow,
                "fiftyTwoWeekRange": fiftyTwoWeekRange,
            }
        )
    df_result = pd.DataFrame(results)
    df_result.to_csv("results_with_highs.csv", index=False)
    end = datetime.now()
    print(f"total time: {end-start}")


def fetch_stock_data(symbol_list: list[str], output_path: str) -> pd.DataFrame:
    rows = []
    all_fields = REQUIRED_FIELDS + OPTIONAL_FIELDS

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

        except Exception as e:
            logging.error(
                f"Error {ticker} while downloading from YF: {e}", exc_info=True
            )
            print(f"Error {ticker} while downloading from YF: {e}", exc_info=True)
            continue

    df = pd.DataFrame(rows)

    df.to_csv(output_path, index=False)
    logging.info(f"Saved {len(df)} tickers to {output_path}")

    return df


def populiting_db(filename):
    try:
        df = pd.read_csv(filename)

        records = []
        for _, row in df.iterrows():
            record = YearHighLow(
                date=None,
                ticker=row["ticker"],
                name=row["name"],
                marketCap=row["marketCap"],
                fiftyTwoWeekHigh=row["fiftyTwoWeekHigh"],
                currentHigh=None,
                newHigh=False,
                fiftyTwoWeekLow=row["fiftyTwoWeekLow"],
                currentLow=None,
                newLow=False,
                fullExchangeName=row["fullExchangeName"],
                fiftyTwoWeekRange=row["fiftyTwoWeekRange"],
            )
            records.append(record)

        session.bulk_save_objects(records)
        session.commit()

        logging.info(f"Database successfully populated. Inserted {len(records)} rows")
        session.close()
    except Exception as e:
        logging.error(f"Database population failed: {e}", exc_info=True)


def merge_df():
    df1 = pd.read_csv("one_million_results_with_highs.csv")
    df2 = pd.read_csv("more_then_one_million_ticker.csv")

    df_merged = pd.merge(
        df1,
        df2[["Symbol", "Name"]],
        left_on="Ticker",
        right_on="Symbol",
        how="left",  # Keep all rows from df1
    )

    # Drop the redundant 'Symbol' column
    df_merged = df_merged.drop(columns=["Symbol"])
    df_merged.to_csv("merged.csv", index=False)
    print(df_merged.head())


def check_new_high_low():

    # TODO: change it to yesterday date
    last_date = date(2026, 4, 8)
    tickers = [row.ticker for row in session.query(YearHighLow.ticker).all()]

    for ticker in tickers[:5]:

        last_day_closing_price = (
            session.query(StockData)
            .filter(
                StockData.ticker == ticker,
                StockData.date == last_date,
            )
            .first()
        )
        if last_day_closing_price:
            print(last_day_closing_price.high)
        # print(f"{last_day_closing_price.high}")
    session.close()
    print(len(tickers))
    print(tickers[:5])


filename = "nasdaq_screener_1775906600921.csv"
df_hundred_million = create_tickers_df_MC_lt100M_from_file(filename)
symbol_list = df_hundred_million["Symbol"].tolist()
# get_52_week_high_from_YF(df_hundred_million)

df = fetch_stock_data(symbol_list=symbol_list, output_path="new_results_with_highs.csv")

print(df.head())
# merge_df()
# populiting_db("merged.csv")
# check_new_high_low()

"""
TODO:
add logs
"""

"""
Workflof:
DONE get all tickers from CSV
READY get from YF all 52 week highs/lows, exchange and put it to new csv
DB
- Add new table 52weekHighsLows
- Populate new table with benchmark values from csv
"""

"""
Code Workflof:
After daily downloading data from YF and populiting DB 
Reset all new_high values to 0/False/Null in table 52weekHighs
Create a list with all tickers from DB
loop through and chech 
"""
