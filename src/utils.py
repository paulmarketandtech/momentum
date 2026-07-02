import logging
import os
from datetime import date, timedelta
from typing import Dict

from dotenv import load_dotenv
from sqlalchemy.orm import Session

from database import get_session
from models.models import AllTickersMonthlyUpdate

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


"""
For now it will stay 2B and 5B 
but in the future 2B will probably be gone and 5B will be dynamic
"""


def creating_list_of_tickers_2B(
    list_of_indexes: list[str],
    list_of_commodities: list[str],
    list_of_etfs: list[str],
    session: Session,
) -> list[str]:
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


def creating_list_of_tickers_5B() -> list[str]:
    list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.market_cap > 5_000_000_000)
        .all()
    ]
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def creating_list_of_tickers_nasdaq() -> list[str]:
    nasdaq_list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.nasdaq_tickers == True)
        .all()
    ]
    return nasdaq_list_of_tickers


def creating_list_of_tickers_nyse() -> list[str]:
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

session = get_session()

list_of_tickers_2B = creating_list_of_tickers_2B(
    list_of_indexes, list_of_commodities, list_of_etfs, session
)
list_of_tickers_5B = creating_list_of_tickers_5B()
list_of_tickers_nasdaq = creating_list_of_tickers_nasdaq()
list_of_tickers_nyse = creating_list_of_tickers_nyse()

session.close()
