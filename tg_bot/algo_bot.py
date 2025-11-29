import os
import logging
import requests
import pandas as pd

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


async def algo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("User %s started the conversation.", update)
    # await update.message.reply_text("info")


async def top200_loosers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        df = pd.read_csv("top_200_loosers.csv")
        top200_loosers_msg = f"Biggest drops from last high\n\n"
        for _, row in df.iterrows():
            top200_loosers_msg += f"{row['ticker']}: {round(row['pct_change'],2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=top200_loosers_msg,
        )
        logging.info("top200_loosers successly sent")
    except Exception as e:
        logger.error("top200_loosers Error: %s", e)


async def weekly_worst(update: Update, context: ContextTypes.DEFAULT_TYPE):
    endpoint = f"http://127.0.0.1:8000/api/weekly-worst/{context.args[0]}"
    response = requests.get(url=endpoint)
    data = response.json()
    await update.message.reply_text(text=data)


application = Application.builder().token(os.getenv("ALGO_BOT_TOKEN")).build()

application.add_handler(CommandHandler("algo", algo))
application.add_handler(CommandHandler("loosers", top200_loosers))
application.add_handler(CommandHandler("ww", weekly_worst))

application.run_polling(allowed_updates=Update.ALL_TYPES)

# TODO:
# https://youtu.be/VV202Rydafc?si=jXLe781MV1r3ZDLp&t=2379
