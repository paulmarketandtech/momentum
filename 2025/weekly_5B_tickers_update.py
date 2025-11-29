from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import os, logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

Base = declarative_base()


"""
WORKFLOW:
0. Download data for all Nasdaq and Nyse tickers from https://www.nasdaq.com/market-activity/stocks/screener
1. Create DF with tickers and Market 
2. Update market_cap in table list_of_tickers_lt_2B
3. Select tickers with market_cap > 5B
4. Delete all records from table list_of_tickers_lt_5B
5. Populate table list_of_tickers_lt_5B with tickers with market_cap > 5B - copy from list_of_tickers_lt_1B
"""


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class TickersList2B(Base):
    __tablename__ = "list_of_tickers_lt_2B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Float, nullable=False)
    nasdaq_tickers = Column(String, nullable=False)
    nyse_tickers = Column(String, nullable=False)

    def __repr__(self):
        return f"<TickersList2B(ticker='{self.ticker}'),(nasdaq='{self.nasdaq_tickers}'),(nyse='{self.nyse_tickers}')>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
# Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


def query_db_length_before():
    query_result_2B = session.query(TickersList2B).all()
    query_result_5B = session.query(TickersList5B).all()
    logging.info(f"Number of tickers lt 2B BEFORE: {len(query_result_2B)}")
    logging.info(f"Number of tickers lt 5B BEFORE: {len(query_result_5B)}")


# Step 1: Create DF with tickers and Market Cap
def create_df_lt_2B(filename):
    try:
        df_csv = pd.read_csv(filename)
        df = df_csv.dropna(subset=["Market Cap"], inplace=False)
        df_two_bills = df[df["Market Cap"] >= 2_000_000_000]
        df_ticker_MC = df_two_bills[["Symbol", "Market Cap"]]
        logging.info("Step 1. DF with tickers and Market Cap created")
        return df_ticker_MC
    except Exception as e:
        logging.error(f"Step1 Error {e}")


# DELETE
# Step 2: Update market_cap in table list_of_tickers_lt_2B
def update_market_cap(df):
    for _, row in df.iterrows():
        session.query(TickersList2B).filter_by(ticker=row["Symbol"]).update(
            {"market_cap": row["Market Cap"]}
        )

    session.commit()
    logging.info("Market Caps updated")


# Step 3: Select tickers with market_cap > 5B
def select_lt_5B():
    lt_5B = (
        session.query(TickersList2B)
        .filter(TickersList2B.market_cap > 5_000_000_000)
        .all()
    )
    logging.info("Tickers with market_cap > 5B selected")
    return lt_5B


# Step 4: Delete all records from table list_of_tickers_lt_5B
def delete_lt_5B():
    session.query(TickersList5B).delete()
    session.commit()
    logging.info("All records deleted")


# Step 5: Populate table list_of_tickers_lt_5B with tickers with market_cap > 5B - copy from list_of_tickers_lt_1B
def populate_lt_5B(lt_5B):
    for ticker in lt_5B:
        stock_price = TickersList5B(
            ticker=ticker.ticker,
            nasdaq_tickers=True if ticker.nasdaq_tickers == "1" else False,
            nyse_tickers=True if ticker.nyse_tickers == "1" else False,
        )
        session.add(stock_price)
    logging.info("Tickers with market_cap > 5B populated")
    session.commit()


def query_db_length_after():
    query_result_5B = session.query(TickersList5B).all()
    logging.info(f"Number of tickers lt 5B AFTER: {len(query_result_5B)}")


def main():
    logging.info("Starting weekly_5B_tickers_update.py")
    query_db_length_before()
    # TODO: Add check if == 1
    number_files = os.listdir(os.getenv("WEEKLY_TICKERS_UPDATE_PATH"))
    uri = f"{os.getenv('WEEKLY_TICKERS_UPDATE_PATH')}/{number_files[0]}"
    df_2B_ticker_MC = create_df_lt_2B(uri)
    update_market_cap(df_2B_ticker_MC)
    lt_5B_tickers = select_lt_5B()
    delete_lt_5B()
    populate_lt_5B(lt_5B_tickers)
    query_db_length_after()
    logging.info("weekly_5B_tickers_update.py finished")


if __name__ == "__main__":
    main()

# TODO: merge with creating_list_of_tickers.py.
"""
Steps:
create a new file and put it all together over there?
clean up and organize creating_list_of... add logs, try-except, create functions
delete step 2 from weekly_5B..
query_db_length_after() - make it a 1 func. dinamic parameter before/after - passing a string
merge with weekly_5B...
2B reads from file. 5B reads from 2B table
"""
