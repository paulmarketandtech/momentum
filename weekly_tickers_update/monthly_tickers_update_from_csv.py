import logging
import os
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    and_,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

Base = declarative_base()


# engine = create_engine(os.getenv("DB_STOCK_DATA")) # dev
engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
Session = sessionmaker(bind=engine)
session = Session()

ABOVE_MARKET_CAP = 200_000_000

"""
Once a month (at the end) dowload manually spreadsheet from website https://www.nasdaq.com/market-activity/stocks/screener
and based on them MC will be updated 
"""


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


Base.metadata.create_all(engine)


# 0. Check DBs length before
def check_query_db_length(before_after):
    query_result = session.query(AllTickersMonthlyUpdate).all()
    logging.info(
        f"Number of all tickers with MC > {ABOVE_MARKET_CAP}: {before_after}: {len(query_result)}"
    )
    print(len(query_result))


# 1. Delete all records from table AllTickersMonthlyUpdate
def delete_tickers_from_all_tickers_monthly():
    try:
        session.query(AllTickersMonthlyUpdate).delete()
        session.commit()
        logging.info("Step 1 done. All records from AllTickersMonthlyUpdate")
    except Exception as e:
        logging.error(f"Step1 Error {e}")


# 2. Getting the name of file with all tickers
def getting_file_name_with_all_tickers():
    string_lenght = []
    try:
        list_files = os.listdir(os.getenv("MONTHLY_TICKERS_UPDATE_PATH"))
        logging.info(f"Step 2 done. Working on file: {list_files[0]}")
        return list_files[0]
    except Exception as e:
        logging.error(f"Step 2A Error: {e}")


# 3. Create DF from a file with tickers with Market Cap > ABOVE_MARKET_CAP
def create_all_tickers_df_from_file(filename):
    try:
        filename_uri = f"{os.getenv('MONTHLY_TICKERS_UPDATE_PATH')}/{filename}"
        df_csv = pd.read_csv(filename_uri)
        df = df_csv.dropna(subset=["Market Cap"], inplace=False)
        df_above_given_MC = df[df["Market Cap"] >= ABOVE_MARKET_CAP]
        df_ticker_MC = df_above_given_MC[["Symbol", "Market Cap"]]
        logging.info(
            f"Step 3 done. DF with tickers and Market Cap > {ABOVE_MARKET_CAP} created"
        )
        return df_ticker_MC
    except Exception as e:
        logging.error(f"Step 3 Error: {e}")


# 4. Populate DB with tickers and Market Cap.
def insert_tickers_allTickers_table(df):
    try:
        for _, row in df.iterrows():
            market_cap = AllTickersMonthlyUpdate(
                ticker=row["Symbol"],
                market_cap=row["Market Cap"],
                date=date.today(),
                nasdaq_tickers=False,
                nyse_tickers=False,
            )
            session.add(market_cap)

        session.commit()
        logging.info(f"Step 4 done. ABOVE_MARKET_CAP {ABOVE_MARKET_CAP} popuated")
    except Exception as e:
        logging.error(f"Step 4 Error: {e}")


def main():
    check_query_db_length("BEFORE")
    delete_tickers_from_all_tickers_monthly()
    filename = getting_file_name_with_all_tickers()
    df_above_given_MC = create_all_tickers_df_from_file(filename)
    insert_tickers_allTickers_table(df_above_given_MC)
    check_query_db_length("AFTER")


if __name__ == "__main__":
    main()
