import logging
import os
import runpy
import time
from datetime import date
from typing import Dict

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from database import get_session
from models.models import StockData
from utils import list_of_tickers_2B, previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


# downloads from YF and write DFs to files
def download_tickers_from_yf(tickers: list[str], last_date: str) -> None:
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

        # check if there is any data,
        # so if there was a day of the whole process should break
        # empty file should crush the process
        df_check = pd.read_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(previous_day).replace('-', '')}.csv",
            engine="python",
        )
        # Count total lines (including header)
        total_lines = len(df_check) + 1

        # if check just in cas if there a lot of missing data
        # also not wanted situation
        if total_lines < 50:
            logging.warning(
                f"VERY FEW LINES. Number of lines: {total_lines}", exc_info=True
            )
            return

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


def read_df_from_csv_and_populate_db(last_date: str, session: Session) -> None:
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
        session = get_session()
        print(f"Working on date: {previous_day}")
        logging.info(f"Working on date: {previous_day}")

        download_tickers_from_yf(list_of_tickers_2B, previous_day)
        read_df_from_csv_and_populate_db(previous_day, session)
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
    main()
