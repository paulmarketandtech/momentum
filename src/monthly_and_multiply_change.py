from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import logging
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

Base = declarative_base()


class SourceData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class MonthlyChange(Base):
    __tablename__ = "monthly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_month_change = Column(Float, nullable=True)
    three_months_change = Column(Float, nullable=True)
    six_months_change = Column(Float, nullable=True)
    twelve_months_change = Column(Float, nullable=True)
    two_months_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<MonthlyChange(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


class Momentum_12_3(Base):
    __tablename__ = "momentum_12_3"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.november05_best})>"


class Momentum_6_2(Base):
    __tablename__ = "momentum_6_2"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.november05_worst})>"


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


engine = create_engine(os.getenv("DB_STOCK_DATA"))
# engine = create_engine(os.getenv("DB_STOCK_DATA_BACKUP"))
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# ----------------------------


def searching_for_last_working_day(no_months):
    format_string = "%Y-%m-%d %H:%M:%S"
    today = datetime.today()
    # today = datetime(2025, 1, 1)
    X_months_ago = today - pd.DateOffset(months=no_months)

    last_day_of_X_months_ago = X_months_ago.date().replace(day=1) - timedelta(days=1)

    week_day_number = last_day_of_X_months_ago.isoweekday()
    if week_day_number == 7:
        last_working_day = last_day_of_X_months_ago - pd.DateOffset(days=2)
    elif week_day_number == 6:
        last_working_day = last_day_of_X_months_ago - pd.DateOffset(days=1)
        # timestamp_string = last_day_of_X_months_ago - pd.DateOffset(days=1)
        # last_working_day = datetime.strptime(timestamp_string, format_string)
        # print("aj", type(last_working_day))
    else:
        last_working_day = last_day_of_X_months_ago
        # print(type(last_working_day))

    # print("Last working day of prev month:", last_working_day)
    return last_working_day


def defining_last_working_days():

    last_working_day_one_month_ago = searching_for_last_working_day(1)
    last_working_day_three_months_ago = searching_for_last_working_day(3)
    last_working_day_six_months_ago = searching_for_last_working_day(6)
    last_working_day_twelve_months_ago = searching_for_last_working_day(11)
    return [
        last_working_day_one_month_ago,
        last_working_day_three_months_ago,
        last_working_day_six_months_ago,
        last_working_day_twelve_months_ago,
    ]


def monthly_change(tickers, list_of_last_working_days):
    print(f"Number of tickers: {len(tickers)}")
    calculated_rows = []
    last_date = date(2025, 2, 28)
    for i, day in enumerate(list_of_last_working_days):
        print(day)
        for j, ticker in enumerate(tickers):
            try:
                if j % 100 == 0:
                    print(i, j)
                last_day_data = (
                    session.query(SourceData)
                    .filter(
                        SourceData.ticker == ticker,
                        SourceData.date == last_date,
                    )
                    .first()
                )
                x_monts_ago_data = (
                    session.query(SourceData)
                    .filter(
                        SourceData.ticker == ticker,
                        SourceData.date == day,
                    )
                    .first()
                )

                x_months_returns = (
                    (last_day_data.close - x_monts_ago_data.close)
                    / x_monts_ago_data.close
                ) * 100
                if i == 0:
                    calculated_rows.append(
                        MonthlyChange(
                            ticker=ticker,
                            date=last_date,
                            one_month_change=x_months_returns,
                            three_months_change=None,
                            six_months_change=None,
                            twelve_months_change=None,
                        )
                    )
                elif i == 1:
                    three_months_return = x_months_returns
                    session.query(MonthlyChange).filter_by(
                        ticker=ticker, date=last_date
                    ).update({"three_months_change": three_months_return})
                    session.commit()
                elif i == 2:
                    six_months_return = x_months_returns
                    session.query(MonthlyChange).filter_by(
                        ticker=ticker, date=last_date
                    ).update({"six_months_change": six_months_return})
                    session.commit()
                elif i == 3:
                    twelve_months_return = x_months_returns
                    session.query(MonthlyChange).filter_by(
                        ticker=ticker, date=last_date
                    ).update({"twelve_months_change": twelve_months_return})
                    session.commit()
                else:
                    print("something went wrong...")

            except AttributeError:
                print("Bad ticker:", ticker)
        if i == 0:
            session.bulk_save_objects(calculated_rows)
            session.commit()


list_of_tickers = [t.ticker for t in session.query(TickersList5B).all()]
# list_of_last_working_days = defining_last_working_days()
# print(list_of_last_working_days)
# monthly_change(list_of_tickers, list_of_last_working_days)


def manual_update(list_of_tickers):
    for ticker in list_of_tickers:
        try:
            last_date = date(2025, 2, 28)
            day = date(2024, 12, 31)
            last_day_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == last_date,
                )
                .first()
            )
            x_monts_ago_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == day,
                )
                .first()
            )
            x_months_returns = (
                (last_day_data.close - x_monts_ago_data.close) / x_monts_ago_data.close
            ) * 100

            two_months_return = x_months_returns
            session.query(MonthlyChange).filter_by(
                ticker=ticker, date=last_date
            ).update({"two_months_change": two_months_return})
        except AttributeError:
            print("Bad ticker:", ticker)
    session.commit()


# manual_update(list_of_tickers)

specific_date = date(2025, 2, 28)
query_result = (
    session.query(
        MonthlyChange.date,
        MonthlyChange.ticker,
        MonthlyChange.two_months_change,
        MonthlyChange.three_months_change,
        MonthlyChange.six_months_change,
        MonthlyChange.twelve_months_change,
    )
    .filter(MonthlyChange.date == specific_date)
    .all()
)

df = pd.DataFrame(query_result, columns=["date", "ticker", "2m", "3m", "6m", "12m"])
df["date"] = pd.to_datetime(df["date"]).dt.date
df = df.dropna()
# df.set_index("date", inplace=True)

df_6m_top100 = df.nlargest(100, "6m")
df_12m_top100 = df.nlargest(100, "12m")
tickers_6m_top100 = df_6m_top100["ticker"].tolist()
tickers_12m_top100 = df_12m_top100["ticker"].tolist()

for t in tickers_12m_top100:
    filtered_3m_df = df[df["ticker"].isin(tickers_12m_top100)][["date", "ticker", "3m"]]

for t in tickers_6m_top100:
    filtered_2m_df = df[df["ticker"].isin(tickers_6m_top100)][["date", "ticker", "2m"]]


sorted_3m_df = filtered_3m_df.sort_values(by="3m", ascending=False).head(20)
sorted_2m_df = filtered_2m_df.sort_values(by="2m", ascending=False).head(20)

for _, row in sorted_3m_df.iterrows():
    stock_data = Momentum_12_3(
        date=row["date"],
        ticker=row["ticker"],
        pct_change=row["3m"],
    )
    session.add(stock_data)

for _, row in sorted_2m_df.iterrows():
    stock_data = Momentum_6_2(
        date=row["date"],
        ticker=row["ticker"],
        pct_change=row["2m"],
    )
    session.add(stock_data)

# session.commit()
# session.close()

# TODO: find all weekly change - since when. Add user input ticker and chart weekly change
