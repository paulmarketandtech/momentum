import logging
import os

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    Table,
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
# metadata = MetaData()


"""
WORKFLOW
updates MC and exchange from YF
list of tickers for 2B and 5B are made in utils 
done?
"""

"""
TODO: 
cp DB from server 
and the whole tickers update:
create a new table,
populate monthly from a file 
run weekly 
and check if utils work 
"""


class AllTickersMonthlyUpdate(Base):
    __tablename__ = "all_tickers_monthly_update"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)  # date when updated
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Integer, nullable=False)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', MC={self.market_cap})>"


# engine = create_engine(os.getenv("DB_STOCK_DATA")) # dev
engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
# Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


# 0. Check DBs length before
def check_query_db_length(before_after):
    query_result_all = session.query(AllTickersMonthlyUpdate).all()
    logging.info(f"Number of tickers all {before_after}: {len(query_result_all)}")


# ONLY this stays?
def update_MC_and_exchanges_from_YF():
    list_of_tickers = [t.ticker for t in session.query(AllTickersMonthlyUpdate).all()]

    nasdaq_ticker = 0
    nyse_ticker = 0
    for ticker in list_of_tickers:
        try:
            info = yf.Ticker(ticker).info
            market_cap = info.get("marketCap")
            exchange_name = (
                info.get("fullExchangeName") or info.get("exchange") or "Unknown"
            )
            if exchange_name == "NasdaqGS" or exchange_name == "NMS":
                nasdaq_ticker = 1
                nyse_ticker = 0

            if exchange_name == "NYSE" or exchange_name == "NYQ":
                nasdaq_ticker = 0
                nyse_ticker = 1

            session.query(AllTickersMonthlyUpdate).filter(
                AllTickersMonthlyUpdate.ticker == ticker
            ).update(
                {
                    AllTickersMonthlyUpdate.market_cap: market_cap,
                    AllTickersMonthlyUpdate.nasdaq_tickers: nasdaq_ticker,
                    AllTickersMonthlyUpdate.nyse_tickers: nyse_ticker,
                }
            )
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            market_cap = None
            exchange_name = None

    session.commit()
    session.close()


def main():
    logging.info("Starting weekly_tickers_update.py")
    check_query_db_length("BEFORE")
    update_MC_and_exchanges_from_YF()

    logging.info("Working on lt2B table")

    check_query_db_length("AFTER")

    logging.info("Finished weekly_tickers_update.py")


if __name__ == "__main__":
    main()
