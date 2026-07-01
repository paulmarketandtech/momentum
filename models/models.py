from sqlalchemy import Boolean, Column, Date, Float, Integer, String

from database import Base

# Base.metadata.create_all(engine)

print("i'm in models.py")


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    ytd = Column(Integer, nullable=True)
    previous_correction = Column(Float, nullable=True)
    last_correction = Column(Float, nullable=True)
    ma50 = Column(Float, nullable=True)
    ma50_above = Column(Boolean, nullable=True)
    ma100 = Column(Float, nullable=True)
    ma100_above = Column(Boolean, nullable=True)
    ma200 = Column(Float, nullable=True)
    ma200_above = Column(Boolean, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class ExtraStockMetricsAndStats(Base):
    __tablename__ = "extra_stock_metrics"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    long_name = Column(String)
    fifty_two_week_high_value = Column(Float, nullable=False)
    date_52week_high = Column(Date, nullable=False)
    fifty_two_week_low_value = Column(Float, nullable=False)
    date_52week_low = Column(Date, nullable=False)
    market_cap = Column(Float)
    beta_value = Column(Float)
    short_percent_of_float = Column(Float)
    short_ratio = Column(Float)
    date_short_interest = Column(Integer)
    full_exchange_name = Column(String)
    fifty_two_week_range = Column(String)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class AllTickersMonthlyUpdate(Base):
    __tablename__ = "all_tickers_monthly_update"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Integer, nullable=False)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', MC={self.market_cap})>"


class YTD20Best(Base):
    __tablename__ = "ytd_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class YTD20Worst(Base):
    __tablename__ = "ytd_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class PreviousCorrectionBest(Base):
    __tablename__ = "previous_correction_best"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class PreviousCorrectionWorst(Base):
    __tablename__ = "previous_correction_worst"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class LastCorrectionBest(Base):
    __tablename__ = "last_correction_best"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class LastCorrectionWorst(Base):
    __tablename__ = "last_correction_worst"

    id = Column(Integer, primary_key=True)
    benchmark_date = Column(Date, nullable=False)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"
