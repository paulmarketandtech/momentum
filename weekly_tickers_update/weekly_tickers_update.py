import logging
import os
import time

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


# engine = create_engine(os.getenv("DB_STOCK_DATA")) # dev
engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
# Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


def check_query_db_length(before_after):
    query_result_all = session.query(AllTickersMonthlyUpdate).all()
    logging.info(f"Number of tickers all {before_after}: {len(query_result_all)}")


def update_MC_and_exchanges_from_YF():
    list_of_tickers = [t.ticker for t in session.query(AllTickersMonthlyUpdate).all()]

    nasdaq_ticker = 0
    nyse_ticker = 0
    for ticker in list_of_tickers:
        time.sleep(0.5)
        try:
            info = yf.Ticker(ticker).info
            market_cap = info.get("marketCap")
            exchange_name = (
                info.get("fullExchangeName") or info.get("exchange") or "Unknown"
            )
            # Nasdaq options in YF: NasdaqGS, NasdaqGM, NasdaCM
            if (
                exchange_name.lower().startswith("nasdaq")
                or exchange_name == "NMS"
                or exchange_name == "NGM"
            ):
                nasdaq_ticker = 1
                nyse_ticker = 0

            # Nyse options in YF: NYSE, Nyse Amrican
            if exchange_name.lower().startswith("nyse") or exchange_name == "NYQ":
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
            logging.warning(f"Error fetching {ticker}: {e}")
            logging.warning(
                f"ticker: {ticker}, MC: {market_cap}, exchange: {exchange_name}\n nasdaq_ticker: {nasdaq_ticker}, nyse_ticker: {nyse_ticker}"
            )
            market_cap = None
            exchange_name = None

    session.commit()
    session.close()


def main():
    logging.info("Starting weekly_tickers_update.py")
    check_query_db_length("BEFORE")
    update_MC_and_exchanges_from_YF()

    check_query_db_length("AFTER")

    logging.info("Finished weekly_tickers_update.py")


if __name__ == "__main__":
    main()
