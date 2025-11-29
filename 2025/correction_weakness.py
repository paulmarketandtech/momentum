from datetime import date, timedelta
from sqlalchemy import create_engine, Column, Integer, String, select
from sqlalchemy import Float, Date, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import and_
import os
import pandas as pd
import logging
import time
from dotenv import load_dotenv

load_dotenv()

# logging.basicConfig(
#     filename=os.getenv("LOG_FILE"),
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )

# logging.info(f"Starting weekly change populating")

Base = declarative_base()


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    november05 = Column(Float, nullable=True)


class CorrectionsWeakest(Base):
    __tablename__ = "corrections_weakest"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    last_closing_price = Column(Float, nullable=False)
    highest_price = Column(Float, nullable=False)
    pct_change = Column(Float, nullable=False)
    highest_price_date = Column(Date, nullable=False)

    def __repr__(self):
        return f"<CorrectionsWeakest(ticker='{self.ticker}', date='{self.date}')>"


class November05Best(Base):
    __tablename__ = "november05_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.november05_best})>"


class November05Worst(Base):
    __tablename__ = "november05_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.november05_worst})>"


engine = create_engine(os.getenv("DB_STOCK_DATA"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

"""
Steps:
Best performing since November 5 DONE
Worst performing since November 5 DONE
Worst performing since peak after November 5
Best performing after March 4
"""


def november_05_top_bottom_50(last_date):
    query_result = (
        session.query(
            StockData.date,
            StockData.ticker,
            StockData.november05,
        )
        .filter(StockData.date == last_date)
        .all()
    )
    df = pd.DataFrame(query_result, columns=["Date", "Ticker", "November05"])
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df = df.dropna()

    df_november05 = df[["Date", "Ticker", "November05"]]
    df_november05_sorted = df_november05.sort_values(by="November05", ascending=False)

    november05_top50 = df_november05_sorted.head(50)
    november05_worst50 = df_november05_sorted.tail(50)
    november05_worst50 = november05_worst50.sort_values(by="November05", ascending=True)

    november05_top50.to_csv(f"./corrections_statistics/november05_top50.csv")
    november05_worst50.to_csv(f"./corrections_statistics/november05_bottom50.csv")


def worst_performing_after_peak(tickers):
    df = pd.DataFrame(
        columns=[
            "ticker",
            "pct_change",
            "max_date",
            "max_price",
            "min_date",
            "min_price",
        ]
    )
    for ticker in tickers:
        try:
            with session:
                stmt = (
                    select(StockData.date, StockData.high, StockData.low)
                    .where(
                        and_(
                            StockData.ticker == ticker,
                            StockData.date >= date(2024, 11, 5),
                        )
                    )
                    .order_by(StockData.date)
                )

                results = session.execute(stmt).all()
                max_date = None
                if results:
                    max_price = max(result[1] for result in results)
                    for i, result in enumerate(results):
                        if result[1] == max_price:
                            max_index_date, max_date = i, result[0]
                            break

                    min_price = min(result[2] for result in results[max_index_date:])
                    for i, result in enumerate(results):
                        if result[2] == min_price:
                            min_index_date, min_date = i, result[0]
                            break

                    pct_change = (min_price - max_price) / max_price * 100
                    row = [ticker, pct_change, max_date, max_price, min_date, min_price]
                    df.loc[len(df)] = row
                else:
                    print("\nNo data found for AAPL in the last 3 months")
        except Exception as e:
            print(f"Bad ticker {ticker}")
            print(f"Error {e}")
    df = df.dropna()
    df_sorted = df.sort_values(by="pct_change", ascending=True)
    df_sorted.to_csv(f"./corrections_statistics/peak_to_bottom.csv", index=False)


def top_200_loosers(filename):
    df = pd.read_csv(filename)
    df_top200 = df.head(200)
    df_top200 = df_top200[["ticker", "pct_change"]]
    df_top200.to_csv("./corrections_statistics/top_200_loosers.csv", index=False)
    # print(df_top200.head())


def first_rebounce(tickers, last_date, dip_date):
    df = pd.DataFrame(columns=["ticker", "pct_change"])
    for ticker in tickers:
        try:
            last_day_closing_price = (
                session.query(StockData)
                .filter(
                    StockData.ticker == ticker,
                    StockData.date == last_date,
                )
                .first()
            )

            dip_date_price = (
                session.query(StockData)
                .filter(
                    StockData.ticker == ticker,
                    StockData.date == dip_date,
                )
                .first()
            )

            since_dip_return = (
                (last_day_closing_price.close - dip_date_price.low) / dip_date_price.low
            ) * 100
            # print(f"{ticker}, {since_dip_return}%")
            row = [ticker, since_dip_return]
            df.loc[len(df)] = row
        except Exception as e:
            print(f"Bad ticker {ticker}")
            print(f"Error {e}")
    df = df.dropna()
    df_sorted = df.sort_values(by="pct_change", ascending=False)
    df_sorted.to_csv(f"./corrections_statistics/march04.csv", index=False)


# algo for best performing
# add to DB, TG bot
list_of_tickers = [t.ticker for t in session.query(TickersList5B).all()]
last_date = date.today() - timedelta(days=3)
searched_dip_date = date(2025, 3, 4)

# november_05_top_bottom_50(last_date)

# worst_performing_after_peak(list_of_tickers)
# print(f"done worst_performing_after_peak")
# # time.sleep(2)
top_200_loosers("./corrections_statistics/peak_to_bottom.csv")
print(f"done top_200_loosers")
time.sleep(2)
df_top200_loosers = pd.read_csv("./corrections_statistics/top_200_loosers.csv")
time.sleep(2)
lst_top200_loosers_tickers = list(df_top200_loosers["ticker"])
first_rebounce(lst_top200_loosers_tickers, last_date, searched_dip_date)
print(f"done first_rebounce")

# previous_day = date.today() - timedelta(days=1)
