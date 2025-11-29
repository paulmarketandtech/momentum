import yfinance as yf
import logging, time, os, runpy
import pandas as pd
from datetime import date
from utils import previous_day, list_of_tickers_2B
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, Date, String, Float, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Working on {previous_day}")

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


# downloads from YF and write DFs to files
def download_tickers_from_yf(tickers, last_date):
    try:
        fifth_length_of_tickers = len(tickers) // 5
        df = yf.download(
            tickers[:fifth_length_of_tickers],
            group_by="Ticker",
            start=last_date,
            end=date.today(),
        )
        df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
        df = df.reset_index()
        df = df.dropna(axis=1, how="all")
        df.to_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
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
                start=last_date,
                end=date.today(),
            )
            df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
            df = df.reset_index()
            df = df.dropna(axis=1, how="all")
            df.to_csv(
                f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
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
                start=last_date,
                end=date.today(),
            )
            df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
            df = df.reset_index()
            df = df.dropna(axis=1, how="all")
            df.to_csv(
                f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
                mode="a",
                index=False,
                header=False,
            )

        print("YF tickers downloaded")
        logging.info("YF API connection successful. Data downloaded.")
    except Exception as e:
        logging.error(f"YF API connection failed: {e}", exc_info=True)


def read_df_from_csv_and_populate_db(last_date):
    try:
        df = pd.read_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
            engine="python",
        )
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        logging.info(f"DF len: {len(df)}")

        for _, row in df.iterrows():
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

        session.commit()
        print("DB Populated")
        logging.info("Database successfully populated.")
    except Exception as e:
        logging.error(f"Database population failed: {e}", exc_info=True)


def main():
    try:
        print(f"Working on date: {previous_day}")
        logging.info(f"Working on date: {previous_day}")

        download_tickers_from_yf(list_of_tickers_2B, previous_day)
        read_df_from_csv_and_populate_db(previous_day)
        session.close()

        logging.info("5 seconds sleep before daily update")
        time.sleep(5)
        try:
            runpy.run_path(path_name=os.getenv("DAILY_UPDATE_PATH"))
        except Exception as e:
            logging.error(
                f"Error in reaching DAILY_UPDATE_PATH script: {e}", exc_info=True
            )

    except Exception as e:
        logging.critical(f"Critical error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
    # engine = create_engine(os.getenv("DB_STOCK_DATA"))  # dev
    # Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    main()
