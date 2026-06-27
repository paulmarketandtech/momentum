import logging, os, time, runpy
from sqlalchemy import create_engine, Column, Integer, Float, Date, Boolean, String
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import and_
from dotenv import load_dotenv
from utils import previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting Market Breadth counting and DB populating")

Base = declarative_base()


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    ma50 = Column(Float, nullable=True)
    ma50_above = Column(Boolean, nullable=True)
    ma100 = Column(Float, nullable=True)
    ma100_above = Column(Boolean, nullable=True)
    ma200 = Column(Float, nullable=True)
    ma200_above = Column(Boolean, nullable=True)

    def __repr__(self):
        return f"<StockData(date='{self.date}')>"


class MarketBreadth(Base):
    __tablename__ = "market_breadth"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)

    ma50_number_of_stocks_above = Column(Integer, nullable=True)
    ma50_number_of_stocks_below = Column(Integer, nullable=True)
    ma50_pct_of_stocks_above = Column(Float, nullable=True)

    ma100_number_of_stocks_above = Column(Integer, nullable=True)
    ma100_number_of_stocks_below = Column(Integer, nullable=True)
    ma100_pct_of_stocks_above = Column(Float, nullable=True)

    ma200_number_of_stocks_above = Column(Integer, nullable=True)
    ma200_number_of_stocks_below = Column(Integer, nullable=True)
    ma200_pct_of_stocks_above = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(date='{self.date}')>"


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def get_change(above, number_of_tickers):
    if above == number_of_tickers:
        return 100.0
    try:
        return (above / number_of_tickers) * 100.0
    except ZeroDivisionError:
        return 0


list_of_tickers = [t.ticker for t in session.query(TickersList5B).all()]
query_ma = (
    session.query(StockData)
    .filter(and_(StockData.ticker.in_(list_of_tickers), StockData.date == previous_day))
    .all()
)

logging.info(f"Number of stocks: {len(query_ma)}")
# ------ 50
above50 = 0
below50 = 0
for ticker in query_ma:
    if ticker.ma50_above == True:
        above50 += 1
    else:
        below50 += 1

logging.info(f"Number of stocks above ma50 {above50}")
logging.info(f"Number of stocks below ma50 {below50}")

market_breadth_50 = get_change(above50, len(query_ma))

# ------ 100
above100 = 0
below100 = 0
for ticker in query_ma:
    if ticker.ma100_above == True:
        above100 += 1
    else:
        below100 += 1

logging.info(f"Number of stocks above ma100 {above100}")
logging.info(f"Number of stocks below ma100 {below100}")

market_breadth_100 = get_change(above100, len(query_ma))

# ------ 200
above200 = 0
below200 = 0
for ticker in query_ma:
    if ticker.ma200_above == True:
        above200 += 1
    else:
        below200 += 1

logging.info(f"Number of stocks above ma200 {above200}")
logging.info(f"Number of stocks below ma200 {below200}")

market_breadth_200 = get_change(above200, len(query_ma))

stock_data = MarketBreadth(
    date=previous_day,
    ma50_number_of_stocks_above=above50,
    ma50_number_of_stocks_below=below50,
    ma50_pct_of_stocks_above=market_breadth_50,
    ma100_number_of_stocks_above=above100,
    ma100_number_of_stocks_below=below100,
    ma100_pct_of_stocks_above=market_breadth_100,
    ma200_number_of_stocks_above=above200,
    ma200_number_of_stocks_below=below200,
    ma200_pct_of_stocks_above=market_breadth_200,
)
session.add(stock_data)
session.commit()

query_result_mb = (
    session.query(MarketBreadth).filter(MarketBreadth.date == previous_day).all()
)
logging.info(
    f"Number of new records in market breadth DB as of {previous_day}: {len(query_result_mb)}"
)

if len(query_result_mb) > 0:
    logging.info("Market Breadth completed successfully.")

    logging.info("5 seconds sleep before creating chart")
    time.sleep(5)
    try:
        runpy.run_path(path_name=os.getenv("CHART_CREATION_PATH"))
    except Exception as e:
        logging.error(
            f"Error in reaching CHART_CREATION_PATH script: {e}", exc_info=True
        )
else:
    logging.info("Market Breadth didn't work out")
    logging.info("5 seconds sleep before counting YTD Corrections")
    time.sleep(5)
    runpy.run_path(path_name=os.getenv("YTD_CORRECTIONS_PATH"))


session.close()
