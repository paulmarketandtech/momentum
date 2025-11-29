#!/usr/bin/python3
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import date, timedelta, datetime
import logging, os

from telegram import Update
from telegram.ext import ContextTypes, Application, CommandHandler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting telegram bot")

print("TG bot started")

logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

Base = declarative_base()


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class YTD20Best(Base):
    __tablename__ = "ytd_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.pct_change})>"


class YTD20Worst(Base):
    __tablename__ = "ytd_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.ytd_worst})>"


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


class Weekly20Best(Base):
    __tablename__ = "weekly_change_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


class Weekly20Worst(Base):
    __tablename__ = "weekly_change_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


class IndexesWeeklyChange(Base):
    __tablename__ = "indexes_weekly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_week_pct_change = Column(Float, nullable=False)
    four_week_pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


class CommoditiesWeeklyChange(Base):
    __tablename__ = "commodities_weekly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_week_pct_change = Column(Float, nullable=False)
    four_week_pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


try:
    engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
    logging.info(f"TG Engine created")
except Exception as e:
    logging.error(f"Engine creation failed: {e}", exc_info=True)

Session = sessionmaker(bind=engine)
session = Session()

previous_day = date.today() - timedelta(days=1)


async def user_info_momentum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("User %s started the conversation.", update)
    await update.message.reply_text("info")


async def tuesday_number_of_tickers(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result_5B = session.query(TickersList5B).all()
        msg = f"Number of tickers this week: {len(query_result_5B)}"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=msg,
        )
        logging.info("tuesday_number_of_tickers successly sent")
    except Exception as e:
        logger.error("tuesday_number_of_tickers Error: %s", e)


async def ytd_top20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                YTD20Best.date,
                YTD20Best.ticker,
                YTD20Best.pct_change,
            )
            .filter(YTD20Best.date == previous_day)
            .all()
        )
        ytd_best_msg = f"Best performing stocks YTD as of {previous_day}\n\n"
        for q in query_result:
            ytd_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=ytd_best_msg,
        )
        logging.info("ytd_top20 successly sent")
    except Exception as e:
        logger.error("ytd_top20 Error: %s", e)


async def ytd_bottom20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                YTD20Worst.date,
                YTD20Worst.ticker,
                YTD20Worst.pct_change,
            )
            .filter(YTD20Worst.date == previous_day)
            .all()
        )
        ytd_worst_msg = f"Worst performing stocks YTD as of {previous_day}\n\n"
        for q in query_result:
            ytd_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=ytd_worst_msg,
        )
        logging.info("ytd_bottom20 successly sent")
    except Exception as e:
        logger.error("ytd_bottom20 Error: %s", e)


async def last_correction_top20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                LastCorrectionBest.date,
                LastCorrectionBest.ticker,
                LastCorrectionBest.pct_change,
            )
            .filter(LastCorrectionBest.date == previous_day)
            .all()
        )
        last_correction_best_msg = (
            f"Best performing stocks since April 7th as of {previous_day}\n\n"
        )
        for q in query_result:
            last_correction_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=last_correction_best_msg,
        )
        logging.info("last_correction_top20 successly sent")
    except Exception as e:
        logger.error("last_correction_top20 Error: %s", e)


async def last_correction_bottom20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                LastCorrectionWorst.date,
                LastCorrectionWorst.ticker,
                LastCorrectionWorst.pct_change,
            )
            .filter(LastCorrectionWorst.date == previous_day)
            .all()
        )
        last_correction_worst_msg = (
            f"Wrost performing stocks since April 7th as of {previous_day}\n\n"
        )
        for q in query_result:
            last_correction_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=last_correction_worst_msg,
        )
        logging.info("last_correction_bottom20 successly sent")
    except Exception as e:
        logger.error("last_correction_bottom20 Error: %s", e)


async def weekly_top20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                Weekly20Best.date,
                Weekly20Best.ticker,
                Weekly20Best.pct_change,
            )
            .filter(Weekly20Best.date == previous_day)
            .all()
        )
        weekly_best_msg = "This week best performing stocks:\n\n"
        for q in query_result:
            weekly_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=weekly_best_msg,
        )
        logging.info("weekly_top20 successly sent")
    except Exception as e:
        logger.error("weekly_top20 Error: %s", e)


async def weekly_bottom20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                Weekly20Worst.date,
                Weekly20Worst.ticker,
                Weekly20Worst.pct_change,
            )
            .filter(Weekly20Worst.date == previous_day)
            .all()
        )
        weekly_worst_msg = "This week worst performing stocks\n\n"
        for q in query_result:
            weekly_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=weekly_worst_msg,
        )
        logging.info("weekly_bottom20 successly sent")
    except Exception as e:
        logger.error("weekly_bottom20 Error: %s", e)


async def weekly_indexes(context: ContextTypes.DEFAULT_TYPE):
    try:
        # ------INDEXES----------
        query_result_indexes = (
            session.query(
                IndexesWeeklyChange.date,
                IndexesWeeklyChange.ticker,
                IndexesWeeklyChange.one_week_pct_change,
                IndexesWeeklyChange.four_week_pct_change,
            )
            .filter(IndexesWeeklyChange.date == previous_day)
            .all()
        )
        weekly_indexes_msg = "This week indexes performance:\n\n"
        weekly_indexes_msg += "          1W  |  4Ws\n"
        for qi in query_result_indexes:
            weekly_indexes_msg += f"{qi.ticker}: {round(qi.one_week_pct_change,2)}% | {round(qi.four_week_pct_change,2)}%\n"

        # ------COMMODITIES---------
        query_result_commodities = (
            session.query(
                CommoditiesWeeklyChange.date,
                CommoditiesWeeklyChange.ticker,
                CommoditiesWeeklyChange.one_week_pct_change,
                CommoditiesWeeklyChange.four_week_pct_change,
            )
            .filter(CommoditiesWeeklyChange.date == previous_day)
            .all()
        )
        weekly_indexes_msg += "\n\nThis week commodities performance:\n\n"
        weekly_indexes_msg += "          1W  |  4Ws\n"
        for qc in query_result_commodities:
            weekly_indexes_msg += f"{qc.ticker}: {round(qc.one_week_pct_change,2)}% | {round(qc.four_week_pct_change,2)}%\n"

        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=weekly_indexes_msg,
        )
        logging.info("weekly_indexes successly sent")
    except Exception as e:
        logger.error("weekly_indexes Error: %s", e)


async def market_breadth(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await context.bot.send_photo(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            photo=f"{os.getenv('MARKET_BREADTH_SCREENS_FOLDER')}/{str(previous_day).replace('-', '')}.png",
        )
        logging.info("market_breadth successly sent")
    except Exception as e:
        logger.error("market_breadth Error: %s", e)
    context.application.stop_running()


application = Application.builder().token(os.getenv("TG_TOKEN")).build()

application.add_handler(CommandHandler("info", user_info_momentum))
logging.info("starting job queue")
job_queue = application.job_queue
today = datetime.today().strftime("%A")
if today.lower() == "tuesday":
    job_queue.run_once(tuesday_number_of_tickers, 1)
if today.lower() == "saturday":
    job_queue.run_once(weekly_indexes, 1)
job_queue.run_once(weekly_top20, 3)
job_queue.run_once(weekly_bottom20, 5)
job_queue.run_once(ytd_top20, 7)
job_queue.run_once(ytd_bottom20, 10)
job_queue.run_once(last_correction_top20, 13)
job_queue.run_once(last_correction_bottom20, 16)
job_queue.run_once(market_breadth, 19)
logging.info("job queue ended")

application.run_polling(allowed_updates=Update.ALL_TYPES)

logging.info(f"Finished TG bot")
