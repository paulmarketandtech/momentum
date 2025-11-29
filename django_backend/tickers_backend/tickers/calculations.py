from .models import YtdBest, YtdWorst, StockData, Momentum123, Momentum62, MonthlyChange
from datetime import date, datetime, timedelta
import pandas as pd


def searching_for_last_working_day():
    today = datetime.today()

    last_day_month_ago = today.replace(day=1) - timedelta(days=1)

    week_day_number = last_day_month_ago.isoweekday()
    if week_day_number == 7:
        last_working_day = last_day_month_ago - pd.DateOffset(days=2)
    elif week_day_number == 6:
        last_working_day = last_day_month_ago - pd.DateOffset(days=1)
    else:
        last_working_day = last_day_month_ago

    return last_working_day.strftime("%Y-%m-%d")


last_working_day_previous_month = searching_for_last_working_day()


def get_best_ytd():
    stocks = YtdBest.objects.filter(date=date(2025, 2, 28)).order_by("-pct_change")[:10]
    ytd_best = []
    for stock in stocks:
        ytd_best.append(
            {"date": stock.date, "ticker": stock.ticker, "pct_change": stock.pct_change}
        )
    return ytd_best


def get_worst_ytd():
    stocks = YtdWorst.objects.filter(date=date(2025, 2, 28)).order_by("pct_change")[:10]
    ytd_worst = []
    for stock in stocks:
        ytd_worst.append(
            {"date": stock.date, "ticker": stock.ticker, "pct_change": stock.pct_change}
        )
    return ytd_worst


def get_momentum_12_3():
    stocks = Momentum123.objects.filter(date=last_working_day_previous_month)[:10]
    momentum_12_3 = []
    for stock in stocks:
        momentum_12_3.append(
            {
                "date": stock.date,
                "ticker": stock.ticker,
                "pct_change": stock.pct_change,
                "twelve_months_change": MonthlyChange.objects.filter(
                    date=stock.date, ticker=stock.ticker
                )
                .first()
                .twelve_months_change,
            }
        )
    return momentum_12_3


def get_momentum_6_2():
    stocks = Momentum62.objects.filter(date=last_working_day_previous_month)[:10]
    momentum_6_2 = []
    for stock in stocks:
        momentum_6_2.append(
            {
                "date": stock.date,
                "ticker": stock.ticker,
                "pct_change": stock.pct_change,
                "six_months_change": MonthlyChange.objects.filter(
                    date=stock.date, ticker=stock.ticker
                )
                .first()
                .six_months_change,
            }
        )
    return momentum_6_2


working_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
weekend_days = ["Saturday", "Sunday"]
holidays = [
    "2025-01-01",
    "2025-01-20",
    "2025-02-17",
    "2025-04-18",
    "2025-05-26",
    "2025-07-04",
    "2025-09-01",
    "2025-11-27",
    "2025-12-25",
]

# date = datetime.today() - timedelta(days=i)

specific_date = date(2025, 2, 28)


def previous_working_day():
    previous_day = datetime.today() - timedelta(days=1)
    i = 1
    while True:
        if previous_day.strftime("%A") in working_days:
            return previous_day
        else:
            i += 1
            previous_day = (datetime.today() - timedelta(days=i)).strftime("%A")


def previous_working_day_data(last_day, symbol):
    i = 1
    while True:
        if last_day.strftime("%A") in working_days:
            yesterday_stock_data = StockData.objects.filter(
                date=last_day, ticker=f"{symbol}"
            ).first()
            break
        else:
            i += 1
            last_day = (datetime.today() - timedelta(days=i)).strftime("%A")

    return yesterday_stock_data


def year_ago_data(last_day, symbol):
    n = 365
    year_ago = last_day - timedelta(days=n)
    while True:
        if year_ago.strftime("%A") in working_days:
            year_ago_stock_data = StockData.objects.filter(
                date=year_ago, ticker=f"{symbol}"
            ).first()
            break
        else:
            n += 1
            year_ago = last_day - timedelta(days=n)

    return year_ago_stock_data


def get_list_of_weekly_change_dates():
    start_date = date(2025, 1, 24)
    lst_dates = []
    while start_date < date.today():
        lst_dates.append(start_date)
        start_date += timedelta(days=7)
    return lst_dates


def get_weekly_change(list_of_fridays, ticker):
    # ticker = "NVDA"
    weekly_changes_list = []
    for date in list_of_fridays:
        weekly_changes_list.append(
            StockData.objects.filter(date=date, ticker=ticker).values_list()[0][17]
        )
    return weekly_changes_list


list_of_fridays = get_list_of_weekly_change_dates()
# list_of_weekly_changes = get_weekly_change(list_of_fridays)

last_working_day = previous_working_day()
# last_close = previous_working_day_data(last_working_day)
# year_ago_close = year_ago_data()
# one_year_return = (last_close.close - year_ago_close.close) / year_ago_close.close * 100
