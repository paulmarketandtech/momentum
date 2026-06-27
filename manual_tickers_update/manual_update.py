import sys
from pathlib import Path

# Add parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(parent_dir))
import logging
import os
import runpy
import time
from datetime import date

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from src.utils import list_of_tickers_2B, previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

Base = declarative_base()


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class AllTickersMonthlyUpdate(Base):
    __tablename__ = "all_tickers_monthly_update"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', MC={self.market_cap})>"


def download_tickers_from_yf(tickers, start_date, end_date):
    try:
        fifth_length_of_tickers = len(tickers) // 5
        df = yf.download(
            tickers[:fifth_length_of_tickers],
            group_by="Ticker",
            start=start_date,
            end=end_date,
        )
        df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
        df = df.reset_index()
        df = df.dropna(axis=1, how="all")
        df.to_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(start_date).replace('-', '')}.csv",
            index=False,
        )

        print("-------------------------------------")
        print("One minute sleep during downloading from YF")
        time.sleep(30)
        print("30 more seconds")
        time.sleep(30)
        print("-------------------------------------")

        for i in range(1, 5):
            beginning = fifth_length_of_tickers * i
            end = fifth_length_of_tickers * (i + 1)
            df = yf.download(
                tickers[beginning:end],
                group_by="Ticker",
                start=start_date,
                end=end_date,
            )
            df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
            df = df.reset_index()
            df = df.dropna(axis=1, how="all")
            df.to_csv(
                f"{os.getenv('CSV_FOLDER_PATH')}/{str(start_date).replace('-', '')}.csv",
                mode="a",
                index=False,
                header=False,
            )

            print("-------------------------------------")
            print("One minute sleep during downloading from YF")
            time.sleep(30)
            print("30 more seconds")
            time.sleep(30)
            print("-------------------------------------")

        if len(tickers) > fifth_length_of_tickers * 5:
            print("last part")
            df = yf.download(
                tickers[fifth_length_of_tickers * 5 :],
                group_by="Ticker",
                start=start_date,
                end=end_date,
            )
            df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
            df = df.reset_index()
            df = df.dropna(axis=1, how="all")
            df.to_csv(
                f"{os.getenv('CSV_FOLDER_PATH')}/{str(start_date).replace('-', '')}.csv",
                mode="a",
                index=False,
                header=False,
            )

        print("YF tickers downloaded")
        logging.info("YF API connection successful. Data downloaded.")
    except Exception as e:
        logging.error(f"YF API connection failed: {e}", exc_info=True)


def read_df_from_csv_and_populate_db_with_missing_data(start_date):
    try:
        df = pd.read_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(start_date).replace('-', '')}.csv",
            engine="python",
        )
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        logging.info(f"DF len: {len(df)}")
        print(f"DF len: {len(df)}")

        i = 0
        for _, row in df.iterrows():
            record = (
                session.query(StockData)
                .filter(
                    StockData.ticker == row["Ticker"], StockData.date == row["Date"]
                )
                .first()
            )

            if record is None:
                stock_price = StockData(
                    date=row["Date"],
                    close=row["Close"],
                    high=row["High"],
                    low=row["Low"],
                    open=row["Open"],
                    volume=row["Volume"],
                    ticker=row["Ticker"],
                )
                session.add(stock_price)
                i += 1

        session.commit()
        session.close()
        print(f"DB Populated. Number of new tickers: {i}")
        logging.info(f"Database successfully populated. Number of new tickers: {i}")
    except Exception as e:
        print(f"Database population failed: {e}")
        logging.error(f"Database population failed: {e}", exc_info=True)


def creating_list_of_tickers(above_given_MC):
    list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.market_cap > above_given_MC)
        .all()
    ]
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


"""
Workflow:
provide start_date and end_date - by default (for now) it will work only for 1 day period 
create list of tickers from All_tickers... table with ABOVE_GIVEN_MC
download data from YF 
populate stock_data with missing data 
"""


def main():
    ABOVE_GIVEN_MC = 500_000_000
    start_date = date(2026, 1, 2)
    end_date = date(2026, 1, 3)

    # list_of_tickers = creating_list_of_tickers(ABOVE_GIVEN_MC)

    try:
        print(f"Working on date: {start_date}")
        logging.info(f"Working on date: {start_date}")

        # download_tickers_from_yf(list_of_tickers, start_date, end_date)
        # read_df_from_csv_and_populate_db_with_missing_data(start_date)

    except Exception as e:
        logging.critical(f"Critical error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
    # engine = create_engine(os.getenv("DB_STOCK_DATA"))  # dev
    # Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    main()
