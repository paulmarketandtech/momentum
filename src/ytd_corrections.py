from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import logging, os
from dotenv import load_dotenv
from utils import previous_day, PREVIOUS_CORRECTION_DATE, LAST_CORRECTION_DATE

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting Best/worst YTD, corrections DB populating")

Base = declarative_base()


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    ytd = Column(Float, nullable=True)
    previous_correction = Column(Float, nullable=True)
    last_correction = Column(Float, nullable=True)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class YTD20Best(Base):
    __tablename__ = "ytd_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class YTD20Worst(Base):
    __tablename__ = "ytd_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class PreviousCorrectionBest(Base):
    __tablename__ = "previous_correction_best"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class PreviousCorrectionWorst(Base):
    __tablename__ = "previous_correction_worst"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class LastCorrectionBest(Base):
    __tablename__ = "last_correction_best"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class LastCorrectionWorst(Base):
    __tablename__ = "last_correction_worst"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

query_result = (
    session.query(
        StockData.date,
        StockData.ticker,
        StockData.ytd,
        StockData.previous_correction,
        StockData.last_correction,
    )
    .filter(StockData.date == previous_day)
    .all()
)
df = pd.DataFrame(
    query_result,
    columns=["Date", "Ticker", "YTD", "Previous_correction", "Last_correction"],
)
df["Date"] = pd.to_datetime(df["Date"]).dt.date

df_ytd = df[["Date", "Ticker", "YTD"]]
df_ytd = df_ytd.dropna()
df_previous_correction = df[["Date", "Ticker", "Previous_correction"]]
df_previous_correction = df_previous_correction.dropna()
df_last_correction = df[["Date", "Ticker", "Last_correction"]]
df_last_correction = df_last_correction.dropna()

df_ytd_sorted = df_ytd.sort_values(by="YTD", ascending=False)
df_previous_correction_sorted = df_previous_correction.sort_values(
    by="Previous_correction", ascending=False
)
df_last_correction_sorted = df_last_correction.sort_values(
    by="Last_correction", ascending=False
)

ytd_top20 = df_ytd_sorted.head(20)
ytd_worst20 = df_ytd_sorted.tail(20)
ytd_worst20 = ytd_worst20.sort_values(by="YTD", ascending=True)

previous_correction_top20 = df_previous_correction_sorted.head(20)
previous_correction_worst20 = df_previous_correction_sorted.tail(20)
previous_correction_worst20 = previous_correction_worst20.sort_values(
    by="Previous_correction", ascending=True
)

last_correction_top20 = df_last_correction_sorted.head(20)
last_correction_worst20 = df_last_correction_sorted.tail(20)
last_correction_worst20 = last_correction_worst20.sort_values(
    by="Last_correction", ascending=True
)


for _, row in ytd_top20.iterrows():
    stock_data = YTD20Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["YTD"],
    )
    session.add(stock_data)

for _, row in ytd_worst20.iterrows():
    stock_data = YTD20Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["YTD"],
    )
    session.add(stock_data)

for _, row in previous_correction_top20.iterrows():
    stock_data = PreviousCorrectionBest(
        benchmark_date=PREVIOUS_CORRECTION_DATE,
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Previous_correction"],
    )
    session.add(stock_data)

for _, row in previous_correction_worst20.iterrows():
    stock_data = PreviousCorrectionWorst(
        benchmark_date=PREVIOUS_CORRECTION_DATE,
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Previous_correction"],
    )
    session.add(stock_data)

for _, row in last_correction_top20.iterrows():
    stock_data = LastCorrectionBest(
        benchmark_date=LAST_CORRECTION_DATE,
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Last_correction"],
    )
    session.add(stock_data)

for _, row in last_correction_worst20.iterrows():
    stock_data = LastCorrectionWorst(
        benchmark_date=LAST_CORRECTION_DATE,
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Last_correction"],
    )
    session.add(stock_data)

session.commit()
session.close()

logging.info(f"Finished Best/worst YTD, corrections DB populating")

import time
import runpy

print("YTD finished - 5 seconds sleepipng")
time.sleep(5)
try:
    runpy.run_path(path_name=os.getenv("WEEKLY_CHANGE_PATH"))
except Exception as e:
    logging.error(f"Error running weekly_change.py: {e}")
