from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy import Boolean, Table, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import os, logging
from sqlalchemy import and_
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

Base = declarative_base()
# metadata = MetaData()

"""
WORKFLOW:
0. Download data for all Nasdaq and Nyse tickers from https://www.nasdaq.com/market-activity/stocks/screener
1. Delete all records from tables list_of_tickers_lt_2B and list_of_tickers_lt_5B
    lt2B table
2. Create DF from a file with tickers with Market Cap > $2B
3. Populate DB with tickers and Market Cap. Set nasdaq_tickers and nyse_tickers to False
4. Update nasdaq_tickers and nyse_tickers to True for Nasdaq and Nyse tickers
5. Delete tickers that are not Nasdaq or Nyse
    lt5B table
Step 6: Select tickers with market_cap > 5B from table lt2B
Step 7: Populate table lt5B with tickers with market_cap > 5B - copied from lt_2B
Last step. Check DBs length after
"""


class TickersList2B(Base):
    __tablename__ = "list_of_tickers_lt_2B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Float, nullable=False)
    nasdaq_tickers = Column(String, nullable=False)
    nyse_tickers = Column(String, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

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
    open = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


# engine = create_engine(os.getenv("DB_STOCK_DATA")) # dev
engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
# Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


# 0. Check DBs length before
def check_query_db_length(before_after):
    query_result_2B = session.query(TickersList2B).all()
    query_result_5B = session.query(TickersList5B).all()
    logging.info(f"Number of tickers lt 2B {before_after}: {len(query_result_2B)}")
    logging.info(f"Number of tickers lt 5B {before_after}: {len(query_result_5B)}")


# 1. Delete all records from tables list_of_tickers_lt_2B and list_of_tickers_lt_5B
def delete_tickers_from_lt2B_and_lt5B():
    try:
        session.query(TickersList2B).delete()
        session.query(TickersList5B).delete()
        session.commit()
        logging.info("Step 1 done. All records from lt_2B & lt_5B deleted")
    except Exception as e:
        logging.error(f"Step1 Error {e}")


# 2A. Getting the name of file with all tickers
def getting_file_name_with_all_tickers():
    string_lenght = []
    try:
        list_files = os.listdir(os.getenv("WEEKLY_TICKERS_UPDATE_PATH"))
        for string in list_files:
            string_lenght.append(len(string))
        logging.info(
            f"Step 2A done. Longest file name is: {list_files[string_lenght.index(max(string_lenght))]}"
        )
        return list_files[string_lenght.index(max(string_lenght))]
    except Exception as e:
        logging.error(f"Step 2A Error: {e}")


# 2B. Create DF from a file with tickers with Market Cap > $2B
def create_tickers_df_MC_lt2B_from_file(filename):
    try:
        filename_uri = f"{os.getenv('WEEKLY_TICKERS_UPDATE_PATH')}/{filename}"
        df_csv = pd.read_csv(filename_uri)
        df = df_csv.dropna(subset=["Market Cap"], inplace=False)
        df_two_bills = df[df["Market Cap"] >= 2_000_000_000]
        df_ticker_MC = df_two_bills[["Symbol", "Market Cap"]]
        logging.info("Step 2B done. DF with tickers and Market Cap created")
        return df_ticker_MC
    except Exception as e:
        logging.error(f"Step 2B Error: {e}")


# 3. Populate DB with tickers and Market Cap. Set nasdaq_tickers and nyse_tickers to False
def insert_tickers_lt2B(df):
    try:
        for _, row in df.iterrows():
            stock_price = TickersList2B(
                ticker=row["Symbol"],
                market_cap=row["Market Cap"],
                nasdaq_tickers=False,
                nyse_tickers=False,
            )
            session.add(stock_price)

        session.commit()
        logging.info("Step 3 done. lt2B popuated with nasdaq/nyse equals False")
    except Exception as e:
        logging.error(f"Step 3 Error: {e}")


# 4. Update nasdaq_tickers and nyse_tickers to True for Nasdaq and Nyse tickers
def update_lt2B_nasdaq_or_nyse(nasdaq_file, nyse_file):
    try:
        df_nasdaq = pd.read_csv(nasdaq_file)
        df_nyse = pd.read_csv(nyse_file)

        for ticker in list(df_nasdaq["Symbol"]):
            session.query(TickersList2B).filter(TickersList2B.ticker == ticker).update(
                {"nasdaq_tickers": True}
            )
        for ticker in list(df_nyse["Symbol"]):
            session.query(TickersList2B).filter(TickersList2B.ticker == ticker).update(
                {"nyse_tickers": True}
            )
        session.commit()
        # print("Step 4 done")
        logging.info(f"Step 4 done. Tickers in lt2B nasdaq or nyse updated")
    except Exception as e:
        logging.error(f"Step 4 Error {e}")


# 5. Delete tickers that are not Nasdaq or Nyse
def delete_not_nasdaq_nor_nyse():
    try:
        results = (
            session.query(TickersList2B)
            .filter(
                and_(
                    TickersList2B.nasdaq_tickers == False,
                    TickersList2B.nyse_tickers == False,
                )
            )
            .delete()
        )
        session.commit()
        logging.info(f"Step 5 done. None Nasdaq or Nyse tickers in lt2B deleted")
    except Exception as e:
        logging.error(f"Step 5 Error {e}")


# Step 6: Select tickers with market_cap > 5B from table lt2B
def select_lt_5B_from_lt2B():
    try:
        lt_5B = (
            session.query(TickersList2B)
            .filter(TickersList2B.market_cap > 5_000_000_000)
            .all()
        )
        logging.info("Step 6 done. Tickers with market_cap > 5B from lt2B selected")
        return lt_5B
    except Exception as e:
        logging.error(f"Step 5 Error {e}")


# Step 7: Populate table lt5B with tickers with market_cap > 5B - copied from lt_2B
def insert_tickers_lt5B(lt_5B):
    try:
        for ticker in lt_5B:
            stock_price = TickersList5B(
                ticker=ticker.ticker,
                nasdaq_tickers=True if ticker.nasdaq_tickers == "1" else False,
                nyse_tickers=True if ticker.nyse_tickers == "1" else False,
            )
            session.add(stock_price)
        session.commit()
        logging.info("Step 7 done. Tickers with market_cap > 5B populated in lt5B")
    except Exception as e:
        logging.error(f"Step 7 Error: {e}")


# Last step. Check DBs length after
def check_query_db_length(before_after):
    query_result_2B = session.query(TickersList2B).all()
    query_result_5B = session.query(TickersList5B).all()
    logging.info(f"Number of tickers lt 2B {before_after}: {len(query_result_2B)}")
    logging.info(f"Number of tickers lt 5B {before_after}: {len(query_result_5B)}")


def main():
    logging.info("Starting weekly_tickers_update.py")
    check_query_db_length("BEFORE")
    delete_tickers_from_lt2B_and_lt5B()

    logging.info("Working on lt2B table")
    # Step 2
    all_tickers_file = getting_file_name_with_all_tickers()
    df_2B = create_tickers_df_MC_lt2B_from_file(all_tickers_file)
    insert_tickers_lt2B(df_2B)
    # Step 4
    nasdaq_file_uri = f"{os.getenv('WEEKLY_TICKERS_UPDATE_PATH')}/nasdaq_lt_2B.csv"
    nyse_file_uri = f"{os.getenv('WEEKLY_TICKERS_UPDATE_PATH')}/nyse_lt_2B.csv"
    update_lt2B_nasdaq_or_nyse(nasdaq_file_uri, nyse_file_uri)
    delete_not_nasdaq_nor_nyse()

    logging.info("Working on lt5B table")
    # Step 6
    lt_5B = select_lt_5B_from_lt2B()
    insert_tickers_lt5B(lt_5B)

    check_query_db_length("AFTER")

    logging.info("Finished weekly_tickers_update.py")


if __name__ == "__main__":
    main()

"""
TODO:
merge to master
delete weekly_5B_tickers_update.py and creating_list_of_tickers.py files
"""
