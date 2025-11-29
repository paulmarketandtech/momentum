import pandas as pd
import yfinance as yf
import logging
from datetime import date, timedelta
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    Boolean,
    case,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from tradingview_ta import TA_Handler, Interval
from sqlalchemy.sql import and_
import os
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


"""
DAILY BOT's Check LIST
Creates list of tickers create_lst_of_tickers(filename)
Downloads data for last day from YF download_tickers_from_yf(tickers, today) and write it to csv....
Creating session
Read DF from CSV and populate DB read_df_from_csv_and_populate_db(date)
Count and populate DB with YTD returns counting_and_populating_ytd_return(tickers, date)
Create Top20 YTD creating_df_best_ytd(last_date)
"""


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
    ytd = Column(Integer, nullable=True)
    # august05 = Column(Float, nullable=True)
    # november05 = Column(Float, nullable=True)
    ma50 = Column(Float, nullable=True)
    ma50_above = Column(Boolean, nullable=True)
    ma100 = Column(Float, nullable=True)
    ma100_above = Column(Boolean, nullable=True)
    ma200 = Column(Float, nullable=True)
    ma200_above = Column(Boolean, nullable=True)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class TickersList1B(Base):
    __tablename__ = "list_of_tickers_lt_1B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Float, nullable=False)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

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


# downloads from YF and write DFs to files
def download_tickers_from_yf(tickers, last_date, end_date):
    try:
        fifth_length_of_tickers = len(tickers) // 5
        df = yf.download(
            tickers[:fifth_length_of_tickers],
            group_by="Ticker",
            start=last_date,
            # end=end_date,
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
                # end=end_date,
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
                # end=end_date,
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


def daily_count_new_records(last_date):
    query_result = session.query(StockData).filter(StockData.date == last_date).all()
    logging.info(f"Number of new records in DB as of {last_date}: {len(query_result)}")
    return len(query_result)


def counting_and_populating_ytd_0805_1105_return(tickers, last_date):
    logging.info("YTD, 0508, 0511 calculations started.")
    for ticker in tickers:
        try:
            last_day_closing_price = (
                session.query(StockData)
                .filter(
                    StockData.ticker == ticker,
                    StockData.date == last_date,
                )
                .first()
            )

            year_opening_price = (
                session.query(StockData)
                .filter(
                    StockData.ticker == ticker,
                    StockData.date == date(2025, 1, 2),
                )
                .first()
            )

            august05_opening_price = (
                session.query(StockData)
                .filter(
                    StockData.ticker == ticker,
                    StockData.date == date(2024, 8, 5),
                )
                .first()
            )

            november05_opening_price = (
                session.query(StockData)
                .filter(
                    StockData.ticker == ticker,
                    StockData.date == date(2024, 11, 5),
                )
                .first()
            )

            ytd_return = (
                (last_day_closing_price.close - year_opening_price.open)
                / year_opening_price.open
            ) * 100
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ytd": ytd_return}
            )

            august05_return = (
                (last_day_closing_price.close - august05_opening_price.open)
                / august05_opening_price.open
            ) * 100
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"august05": august05_return}
            )

            november05_return = (
                (last_day_closing_price.close - november05_opening_price.open)
                / november05_opening_price.open
            ) * 100
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"november05": november05_return}
            )

            session.commit()
        except AttributeError as e:
            logging.error(
                f"Error with {ticker} in YTD calculations: {e}", exc_info=True
            )

    logging.info("YTD, 0508, 0511 calculations completed successfully.")
    print("ytd_0805_1105_return counted")


def nasdaq_counting_and_populating_DB_with_SMAs(last_date):
    logging.info("Nasdaq SMAa calculations started.")
    nasdaq_list_of_tickers = [
        t.ticker
        for t in session.query(TickersList5B)
        .filter(TickersList5B.nasdaq_tickers == True)
        .all()
    ]
    for ticker in nasdaq_list_of_tickers:
        time.sleep(0.15)
        try:
            stock_ticker = TA_Handler(
                symbol=ticker,
                screener="america",
                exchange="NASDAQ",
                interval=Interval.INTERVAL_1_DAY,
            )
            indicators = stock_ticker.get_analysis().indicators
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ma50": indicators["SMA50"]}
            )
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ma100": indicators["SMA100"]}
            )
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ma200": indicators["SMA200"]}
            )
            session.commit()

        except Exception as e:
            logging.error(
                f"Error with {ticker} in populating Nasdaq SMAs/Bad ticker {e}",
                exc_info=True,
            )
    logging.info("Nasdaq SMAa populated successfully.")
    print("Nasdaq SMAa populated")


def nyse_counting_and_populating_DB_with_SMAs(last_date):
    logging.info("Nyse SMAa calculations started.")
    nyse_list_of_tickers = [
        t.ticker
        for t in session.query(TickersList5B)
        .filter(TickersList5B.nyse_tickers == True)
        .all()
    ]
    for ticker in nyse_list_of_tickers:
        time.sleep(0.15)
        try:
            stock_ticker = TA_Handler(
                symbol=ticker,
                screener="america",
                exchange="NYSE",
                interval=Interval.INTERVAL_1_DAY,
            )
            indicators = stock_ticker.get_analysis().indicators
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ma50": indicators["SMA50"]}
            )
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ma100": indicators["SMA100"]}
            )
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {"ma200": indicators["SMA200"]}
            )
            session.commit()

        except Exception as e:
            logging.error(
                f"Error with {ticker} in populating Nyse SMAs/Bad ticker {e}",
                exc_info=True,
            )
    logging.info("Nyse SMAa populated successfully.")
    print("Nyse SMAa populated")


def check_above_below_sma(tickers, last_date):
    logging.info("Above/below SMAs counting started.")
    for ticker in tickers:
        try:
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {
                    "ma50_above": case(
                        (
                            and_(
                                StockData.ma50.isnot(None),
                                StockData.close > StockData.ma50,
                            ),
                            True,
                        ),
                        (StockData.ma50.is_(None), False),
                        else_=False,
                    )
                },
                synchronize_session=False,
            )

            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {
                    "ma100_above": case(
                        (
                            and_(
                                StockData.ma100.isnot(None),
                                StockData.close > StockData.ma100,
                            ),
                            True,
                        ),
                        (StockData.ma100.is_(None), False),
                        else_=False,
                    )
                },
                synchronize_session=False,
            )

            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {
                    "ma200_above": case(
                        (
                            and_(
                                StockData.ma200.isnot(None),
                                StockData.close > StockData.ma200,
                            ),
                            True,
                        ),
                        (StockData.ma200.is_(None), False),
                        else_=False,
                    )
                },
                synchronize_session=False,
            )

            session.commit()

        except Exception as e:
            logging.error(
                f"Error in counting above/below SMAs/Bad ticker {e}", exc_info=True
            )
    logging.info("Above/below SMAs counted.")
    print("Above/below SMAs counted")


def main():
    try:
        # previous_day = date.today() - timedelta(days=1)
        previous_day = date(2025, 4, 7)
        end_date = date(2025, 3, 29)
        print(f"Working on date: {previous_day}")
        logging.info(f"Working on date: {previous_day}")
        # list_of_tickers = [t.ticker for t in session.query(TickersList1B).all()]
        # logging.info(
        #     f"Created list of tickers from DB with length: {len(list_of_tickers)}"
        # )
        # print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
        # download_tickers_from_yf(list_of_tickers, previous_day, end_date)
        # read_df_from_csv_and_populate_db(previous_day)
        # number_of_new_records_in_DB = daily_count_new_records(previous_day)
        # if number_of_new_records_in_DB > 0:
        #     counting_and_populating_ytd_0805_1105_return(list_of_tickers, previous_day)

        #     nasdaq_counting_and_populating_DB_with_SMAs(previous_day)
        #     # It takes about 270 sec
        #     nyse_counting_and_populating_DB_with_SMAs(previous_day)

        #     check_above_below_sma(list_of_tickers, previous_day)

        #     logging.info("All steps completed successfully.")

        #     logging.info("5 seconds sleep before counting market breadth")
        #     time.sleep(5)
        #     try:
        #         runpy.run_path(path_name=os.getenv("MARKET_BREADTH_PATH"))
        #     except Exception as e:
        #         logging.error(
        #             f"Error in reaching MARKET_BREADTH_PATH script: {e}", exc_info=True
        #         )

    except Exception as e:
        logging.critical(f"Critical error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    engine = create_engine(os.getenv("DB_STOCK_DATA"))
    # Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    main()

    session.close()


# bad_tickers = ['BGNE', 'CBOE', 'CET', 'EXEEL', 'IMO'] bad for SMA
