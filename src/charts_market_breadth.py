import logging
import os
import runpy
import time

from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base, sessionmaker

from database import get_session
from models.models import MarketBreadth
from utils import previous_day

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.info("Starting chart ploting")

session = get_session()

query50 = session.query(MarketBreadth.ma50_pct_of_stocks_above).all()
lst50 = []
for value in query50:
    lst50.append(value[0])

query100 = session.query(MarketBreadth.ma100_pct_of_stocks_above).all()
lst100 = []
for value in query100:
    lst100.append(value[0])

query200 = session.query(MarketBreadth.ma200_pct_of_stocks_above).all()
lst200 = []
for value in query200:
    lst200.append(value[0])

query_dates = session.query(MarketBreadth.date).all()
lst_dates = []
for value in query_dates:
    lst_dates.append(value[0].strftime("%Y-%m-%d"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

x = lst_dates
y1 = lst50
y2 = lst100
y3 = lst200

try:
    fig, ax = plt.subplots(figsize=(12, 8))

    ax.plot(x, y1, linewidth=2.0, label="ma50")
    ax.plot(x, y2, linewidth=2.0, label="ma100")
    ax.plot(x, y3, linewidth=2.0, label="ma200")

    ax.set_xticks(x[::15])
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, linestyle="--", alpha=0.7)

    ax.legend()
    plt.title("Market Breadth")
    plt.ylabel("percentage of stocks above MA")
    plt.savefig(
        f"{os.getenv('MARKET_BREADTH_SCREENS_FOLDER')}/{str(previous_day).replace('-', '')}.png"
    )

    logging.info("Chart created successfully.")
except Exception as e:
    logging.info(f"Chart went wrong. Error: {e}")


logging.info("10 seconds sleep before counting YTD Corrections")
time.sleep(10)
try:
    runpy.run_path(path_name=os.getenv("YTD_CORRECTIONS_PATH"))
except Exception as e:
    logging.error(f"Error in reaching YTD_CORRECTIONS script: {e}", exc_info=True)
