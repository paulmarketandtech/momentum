from django.http import HttpResponse
from django.shortcuts import render
from datetime import date, datetime, timedelta
from .models import StockData
from .calculations import (
    get_best_ytd,
    get_worst_ytd,
    last_working_day,
    previous_working_day_data,
    year_ago_data,
    get_momentum_12_3,
    get_momentum_6_2,
    list_of_fridays,
    # list_of_weekly_changes,
    get_weekly_change,
    last_working_day_previous_month,
)


def home(request):
    ytd_best = get_best_ytd
    ytd_worst = get_worst_ytd
    momentum_12_3 = get_momentum_12_3
    momentum_6_2 = get_momentum_6_2
    context = {
        "ytd_best": ytd_best,
        "ytd_worst": ytd_worst,
        "momentum_12_3": momentum_12_3,
        "momentum_6_2": momentum_6_2,
        "last_working_day_previous_month": last_working_day_previous_month,
    }
    return render(request, "tickers/index.html", context=context)


import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from io import StringIO, BytesIO


def chart_view(request, ticker):
    plt.style.use("dark_background")
    fig, ax = plt.subplots()
    ax.bar(
        list_of_fridays, get_weekly_change(list_of_fridays, ticker), label="Sample Data"
    )
    ax.set_title("Weekly Changes")
    ax.set_xlabel("EOW date")
    ax.set_ylabel("Percentage Change")
    ax.set_xticks(list_of_fridays[::])
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, linestyle="--", alpha=0.7)
    ax.legend()

    # Save the plot to an in-memory buffer
    buf = BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)

    # Return the image as an HTTP response
    return HttpResponse(buf.getvalue(), content_type="image/png")


def single_ticker(request):
    ticker_data = None
    ticker_input = ""
    last_close = ""
    one_year_return = 0
    if request.method == "POST":
        ticker_input = request.POST.get("ticker", "").upper()
        stock = StockData.objects.filter(
            ticker=ticker_input, date=datetime.today() - timedelta(days=1)
        ).first()
        last_close = previous_working_day_data(last_working_day, ticker_input)
        year_ago_close = year_ago_data(last_working_day, ticker_input)
        one_year_return = (
            (last_close.close - year_ago_close.close) / year_ago_close.close * 100
        )
        if stock:
            ticker_data = {
                "date": stock.date,
                "ticker": stock.ticker,
                "price": stock.close,
                "ytd_return": stock.ytd,
            }
    context = {
        "last_working_day": last_close,
        "one_year_return": one_year_return,
        "ticker_data": ticker_data,
        "ticker_input": ticker_input,
        "list_of_fridays": list_of_fridays,
    }
    return render(request, "tickers/single.html", context=context)
