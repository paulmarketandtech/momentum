from ninja import ModelSchema, Schema
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


class TickerListSchema(ModelSchema):
    class Meta:
        model = ListOfTickersLt10B
        fields = ("ticker", "nasdaq_tickers", "nyse_tickers")


class YtdBestSchema(ModelSchema):
    class Meta:
        model = YtdBest
        fields = ("date", "ticker", "pct_change")


class YtdWorstSchema(ModelSchema):
    class Meta:
        model = YtdWorst
        fields = ("date", "ticker", "pct_change")


class August05BestSchema(ModelSchema):
    class Meta:
        model = August05Best
        fields = ("date", "ticker", "pct_change")


class August05worstSchema(ModelSchema):
    class Meta:
        model = August05Worst
        fields = ("date", "ticker", "pct_change")


class November05BestSchema(ModelSchema):
    class Meta:
        model = November05Best
        fields = ("date", "ticker", "pct_change")


class November05WorstSchema(ModelSchema):
    class Meta:
        model = November05Worst
        fields = ("date", "ticker", "pct_change")


class WeeklyChangeBestSchema(ModelSchema):
    class Meta:
        model = WeeklyChangeBest
        fields = ("date", "ticker", "pct_change")


class WeeklyChangeWorstSchema(ModelSchema):
    class Meta:
        model = WeeklyChangeWorst
        fields = ("date", "ticker", "pct_change")


# class MarketBreadth(ModelSchema):
#     # 20250117 first date
#     class Meta:
#         pass
