import logging
import os
from datetime import date, timedelta

from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

previous_day = date.today() - timedelta(days=1)

YTD_DATE = date(2026, 1, 2)
PREVIOUS_CORRECTION_DATE = date(2024, 11, 5)
LAST_CORRECTION_DATE = date(2025, 4, 7)

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))  # prod
# engine = create_engine(os.getenv("DB_STOCK_DATA"))  # dev
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


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


"""
For now it will stay 2B and 5B 
but in the future 2B will probably be gone and 5B will be dynamic
"""


def creating_list_of_tickers_2B(list_of_indexes, list_of_commodities, list_of_etfs):
    list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.market_cap > 2_000_000_000)
        .all()
    ]
    list_of_tickers.extend(list_of_indexes)
    list_of_tickers.extend(list_of_commodities)
    list_of_tickers.extend(list_of_etfs)
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def creating_list_of_tickers_5B():
    list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.market_cap > 5_000_000_000)
        .all()
    ]
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def creating_list_of_tickers_nasdaq():
    nasdaq_list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.nasdaq_tickers == True)
        .all()
    ]
    return nasdaq_list_of_tickers


def creating_list_of_tickers_nyse():
    nyse_list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.nyse_tickers == True)
        .all()
    ]
    return nyse_list_of_tickers


# create tables with those tickers?
list_of_indexes = [
    "QQQ",
    "SPY",
    "DIA",
    "IWM",
    "DAX",
    "EWQ",
    "EWU",
    "EWC",
    "EWZ",
    "ARGT",
    "EWW",
    "EWA",
    "MCHI",
    "KWEB",
    "EWJ",
    "EPI",
    "EWY",
    "EWT",
    "EWH",
    "EWS",
]
list_of_commodities = ["GLD", "SLV", "COPX", "USO"]

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
    "ROBO",
    "BOTZ",
]


list_of_tickers_2B = creating_list_of_tickers_2B(
    list_of_indexes, list_of_commodities, list_of_etfs
)
list_of_tickers_5B = creating_list_of_tickers_5B()
list_of_tickers_nasdaq = creating_list_of_tickers_nasdaq()
list_of_tickers_nyse = creating_list_of_tickers_nyse()

session.close()
