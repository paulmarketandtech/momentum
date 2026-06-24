import logging
import os

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
# Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

ABOVE_MARKET_CAP = 100_000_000

"""
Once a month I will download manually csv with all tickers
and based on them MC will be updated 
"""


class AllTickersMonthlyUpdate(Base):
    __tablename__ = "all_tickers_monthly_update"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)  # date when updated
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', MC={self.market_cap})>"


# 0. Check DBs length before
def check_query_db_length(before_after):
    query_result = session.query(AllTickersMonthlyUpdate).all()
    logging.info(
        f"Number of all tickers with MC > {ABOVE_MARKET_CAP}: {before_after}: {len(query_result)}"
    )


# 1. Delete all records from tables list_of_tickers_lt_2B and list_of_tickers_lt_5B
def delete_tickers_from_all_tickers_monthly():
    try:
        session.query(AllTickersMonthlyUpdate).delete()
        session.commit()
        logging.info("Step 1 done. All records from AllTickersMonthlyUpdate")
    except Exception as e:
        logging.error(f"Step1 Error {e}")


# 2A. Getting the name of file with all tickers
def getting_file_name_with_all_tickers():
    string_lenght = []
    try:
        list_files = os.listdir(os.getenv("MONTHLY_TICKERS_UPDATE_PATH"))
        logging.info(f"Step 2A done. Working on file: {list_files[0]}")
        return list_files[0]
    except Exception as e:
        logging.error(f"Step 2A Error: {e}")


# 2B. Create DF from a file with tickers with Market Cap > ABOVE_MARKET_CAP
def create_all_tickers_df_from_file(filename):
    try:
        filename_uri = f"{os.getenv('MONTHLY_TICKERS_UPDATE_PATH')}/{filename}"
        df_csv = pd.read_csv(filename_uri)
        df = df_csv.dropna(subset=["Market Cap"], inplace=False)
        df_above_given_MC = df[df["Market Cap"] >= ABOVE_MARKET_CAP]
        df_ticker_MC = df_above_given_MC[["Symbol", "Market Cap"]]
        logging.info("Step 2B done. DF with tickers and Market Cap created")
        return df_ticker_MC
    except Exception as e:
        logging.error(f"Step 2B Error: {e}")


# 3. Populate DB with tickers and Market Cap.
def insert_tickers_lt2B(df):
    try:
        for _, row in df.iterrows():
            market_cap = AllTickersMonthlyUpdate(
                ticker=row["Symbol"],
                market_cap=row["Market Cap"],
                date=date.today(),
            )
            session.add(market_cap)

        session.commit()
        logging.info(f"Step 3 done. ABOVE_MARKET_CAP {ABOVE_MARKET_CAP} popuated")
    except Exception as e:
        logging.error(f"Step 3 Error: {e}")
