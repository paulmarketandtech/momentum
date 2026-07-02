import logging
import os
from datetime import date, datetime
from typing import Dict

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from database import get_session
from models.models import AllTickersMonthlyUpdate, ExtraStockMetricsAndStats
from src.utils import list_of_tickers_2B, previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# pd.set_option("display.float_format", lambda x: f"{x:.0f}" if isinstance(x, (int, float)) else x)

logging.info(f"Starting Extra Stock Metrics for {previous_day}")

REQUIRED_FIELDS = ["fiftyTwoWeekHigh", "fiftyTwoWeekLow"]
OPTIONAL_FIELDS = [
    "marketCap",
    "longName",
    "fiftyTwoWeekRange",
    "fullExchangeName",
    "shortPercentOfFloat",
    "shortRatio",
    "beta",
    "dateShortInterest",
]

"""
TODO: 
Probably to be deleted once everything will be running
ABOVE_GIVEN_MC = 2_000_000_000
def creating_list_of_all_tickers(above_given_MC: int, session: Session):
    list_of_tickers = [
        t.ticker
        for t in session.query(AllTickersMonthlyUpdate)
        .filter(AllTickersMonthlyUpdate.market_cap > above_given_MC)
        .all()
    ]

    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers
"""


def fetch_stock_data(symbol_list: list[str]) -> pd.DataFrame:
    rows = []
    all_fields = REQUIRED_FIELDS + OPTIONAL_FIELDS

    start = datetime.now()
    for i, ticker in enumerate(symbol_list):
        if (i + 1) % 500 == 0:
            logging.info(f"Processing {i + 1}/{len(symbol_list)}")
            logging.info(datetime.now() - start)
            print(f"Processing {i + 1}/{len(symbol_list)}")
            print(datetime.now() - start)

        try:
            info = yf.Ticker(ticker).info

            missing_required = [
                field
                for field in REQUIRED_FIELDS
                if field not in info or info[field] is None
            ]

            if missing_required:
                logging.warning(
                    f"{ticker}: missing required fields {missing_required}, skipping"
                )
                continue

            row = {"ticker": ticker}
            for field in all_fields:
                row[field] = info.get(field)

            rows.append(row)

        except Exception as e:
            logging.error(
                f"Error {ticker} while downloading from YF: {e}", exc_info=True
            )
            continue

    df = pd.DataFrame(rows)

    end = datetime.now()
    logging.info(f"total time: {end-start}")
    return df


def _insert_new_ticker(row: pd.Series, session: Session, previous_day: str) -> None:
    session.add(
        ExtraStockMetricsAndStats(
            ticker=row["ticker"],
            long_name=row["longName"],
            fifty_two_week_high_value=row["fiftyTwoWeekHigh"],
            date_52week_high=previous_day,
            fifty_two_week_low_value=row["fiftyTwoWeekLow"],
            date_52week_low=previous_day,
            market_cap=row["marketCap"],
            fifty_two_week_range=row["fiftyTwoWeekRange"],
            full_exchange_name=row["fullExchangeName"],
            beta_value=row["beta"],
            short_ratio=row["shortRatio"],
            short_percent_of_float=row["shortPercentOfFloat"],
            date_short_interest=row["dateShortInterest"],
        )
    )
    logging.info(f"NEW TICKER. Inserted {row['ticker']}")


def _upsert_metadata(row: pd.Series, record: ExtraStockMetricsAndStats) -> None:
    record.long_name = row["longName"]
    record.market_cap = row["marketCap"]
    record.fifty_two_week_range = row["fiftyTwoWeekRange"]
    record.full_exchange_name = row["fullExchangeName"]
    record.beta_value = row["beta"]
    record.short_ratio = row["shortRatio"]
    record.short_percent_of_float = row["shortPercentOfFloat"]
    record.date_short_interest = row["dateShortInterest"]


def _update_52_week_extremes(
    row: pd.Series, record: ExtraStockMetricsAndStats, previous_day: str
) -> None:
    """
    there are no else statements becasue if is not true then nothing happens
    """
    ticker = row["ticker"]
    currentHigh = record.fifty_two_week_high_value
    newHighValue = row["fiftyTwoWeekHigh"]

    currentLow = record.fifty_two_week_low_value
    newLowValue = row["fiftyTwoWeekLow"]

    if currentHigh is None or newHighValue > currentHigh:
        record.fifty_two_week_high_value = newHighValue
        record.date_52week_high = previous_day
        logging.info(
            f"NEW HIGH. Updated {ticker}: high {currentHigh} → {newHighValue} on {previous_day}"
        )

    if currentLow is None or newLowValue < currentLow:
        record.fifty_two_week_low_value = newLowValue
        record.date_52week_low = previous_day
        logging.info(
            f"NEW LOW. Updated {ticker}: low {currentLow} → {newLowValue} on {previous_day}"
        )


def _process_ticker(
    row: pd.Series,
    session: Session,
    previous_day: str,
    records: Dict[str, ExtraStockMetricsAndStats],
) -> None:
    ticker = row["ticker"]
    if ticker not in records:
        _insert_new_ticker(row, session, previous_day)
    else:
        record = records[ticker]
        _upsert_metadata(row, record)
        _update_52_week_extremes(row, record, previous_day)


def update_stock_metrics(df: pd.DataFrame, session: Session, previous_day: str):
    records: Dict[str, ExtraStockMetricsAndStats] = {
        r.ticker: r for r in session.query(ExtraStockMetricsAndStats).all()
    }
    for _, row in df.iterrows():
        try:
            with session.begin_nested():  # savepoint – auto rollback on exception
                _process_ticker(row, session, previous_day, records)
        except Exception as e:
            logging.error(f"Skipping {row['ticker']}: {e}", exc_info=True)
            session.rollback()
    session.commit()


"""
use those in a output/front end, here (in DB) store raw values
formatted_shortPercentOfFloat = round(shortPercentOfFloat * 100, 2)
formatted_dateShortInterest = datetime.fromtimestamp(
    dateShortInterest
).strftime("%Y-%m-%d")
"""


def main():
    session = get_session()
    symbol_list = list_of_tickers_2B
    df_tickers = fetch_stock_data(symbol_list)
    update_stock_metrics(df_tickers, session, previous_day)

    session.close()


if __name__ == "__main__":
    main()
