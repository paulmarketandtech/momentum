import logging
import os
import runpy
import time
from datetime import date

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from utils import list_of_tickers_2B, previous_day

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
        df = yf.download(
            tickers,
            group_by="Ticker",
            start=last_date,
            end=date(2025, 12, 2),
            # end=date.today(),
        )
        df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
        df = df.reset_index()
        df = df.dropna(axis=1, how="all")
        df.to_csv(
            f"daily_data_csv/{str(last_date).replace('-', '')}.csv",
            index=False,
        )

        print("YF tickers downloaded")
        logging.info("YF API connection successful. Data downloaded.")
    except Exception as e:
        logging.error(f"YF API connection failed: {e}", exc_info=True)


def read_df_from_csv_and_populate_db(last_date):
    try:
        df = pd.read_csv(
            f"daily_data_csv/{str(last_date).replace('-', '')}.csv",
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


list_of_etfs = [
    "XLC",
    "VOX",
    "IYZ",
    "FCOM",
    "XLY",
    "VCR",
    "IYC",
    "FDIS",
    "XLP",
    "VDC",
    "IYK",
    "FSTA",
    "XLE",
    "VDE",
    "IYE",
    "FENY",
    "XLF",
    "VFH",
    "IYF",
    "FNCL",
    "XLV",
    "VHT",
    "IYH",
    "FHLC",
    "XLI",
    "VIS",
    "IYJ",
    "FIDU",
    "XLK",
    "VGT",
    "IYW",
    "FTEC",
    "XLB",
    "VAW",
    "IYM",
    "FMAT",
    "XLRE",
    "VNQ",
    "IYR",
    "FREL",
    "XLU",
    "VPU",
    "IDU",
    "FUTY",
    "IBUY",
    "FINX",
    "IBB",
    "IDNA",
    "IHI",
    "ITA",
    "SOXX",
    "IGV",
    "CIBR",
    "PICK",
    "ICF",
    "ICLN",
    "PAVE",
    "IFRA",
    "SMH",
    "XBI",
    "XHB",
    "ITB",
    "KRE",
    "XOP",
    "GDX",
    "XAR",
    "HACK",
    "TAN",
]

last_date = date(2025, 10, 28)

# download_tickers_from_yf(list_of_etfs, last_date)

engine = create_engine(os.getenv("DB_STOCK_DATA"))  # dev

Session = sessionmaker(bind=engine)
session = Session()
# read_df_from_csv_and_populate_db(last_date)
session.close()


# add IREN CIFR - DONE
# download yf data for jan 1 - DONE
# remove IREN CIFR - DONE
# download yf data from Oct 31 to Nov 27
# populate DB
# add etf list to tickers
