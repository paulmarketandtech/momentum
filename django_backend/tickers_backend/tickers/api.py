from ninja import NinjaAPI
from django.shortcuts import get_list_or_404
from .models import (
    ListOfTickersLt10B,
    YtdBest,
    YtdWorst,
    August05Best,
    August05Worst,
    November05Best,
    November05Worst,
    WeeklyChangeBest,
    WeeklyChangeWorst,
)
from .schemas import (
    TickerListSchema,
    YtdBestSchema,
    YtdWorstSchema,
    August05BestSchema,
    August05worstSchema,
    November05BestSchema,
    November05WorstSchema,
    WeeklyChangeBestSchema,
    WeeklyChangeWorstSchema,
)
from datetime import date

app = NinjaAPI()


@app.get("tickers/", response=list[TickerListSchema])
def get_tickers(request):
    list_of_all_tickers = ListOfTickersLt10B.objects.all()
    return list_of_all_tickers[:5]


@app.get("ytd-best/{date}/", response=list[YtdBestSchema])
def get_ytd_best(request, date: date):
    best_ytd = get_list_or_404(YtdBest, date=date)
    return best_ytd


@app.get("ytd-worst/{date}/", response=list[YtdWorstSchema])
def get_ytd_worst(request, date: date):
    worst_ytd = get_list_or_404(YtdWorst, date=date)
    return worst_ytd


@app.get("august05-best/{date}/", response=list[August05BestSchema])
def get_august05_best(request, date: date):
    best_ytd = get_list_or_404(August05Best, date=date)
    return best_ytd


@app.get("august05-worst/{date}/", response=list[August05worstSchema])
def get_august05_worst(request, date: date):
    best_ytd = get_list_or_404(August05Worst, date=date)
    return best_ytd


@app.get("november05-best/{date}/", response=list[November05BestSchema])
def get_november05_best(request, date: date):
    best_ytd = get_list_or_404(November05Best, date=date)
    return best_ytd


@app.get("november05-worst/{date}/", response=list[November05WorstSchema])
def get_november05_worst(request, date: date):
    best_ytd = get_list_or_404(November05Worst, date=date)
    return best_ytd


@app.get("weekly-best/{date}/", response=list[WeeklyChangeBestSchema])
def get_weekly_best(request, date: date):
    best_ytd = get_list_or_404(WeeklyChangeBest, date=date)
    return best_ytd


@app.get("weekly-worst/{date}/", response=list[WeeklyChangeWorstSchema])
def get_weekly_worst(request, date: date):
    best_ytd = get_list_or_404(WeeklyChangeWorst, date=date)
    return best_ytd
