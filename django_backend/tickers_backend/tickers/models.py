# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class August05Best(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "august05_best"


class August05Worst(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "august05_worst"


class ListOfTickersLt10B(models.Model):
    id = models.AutoField(primary_key=True)
    ticker = models.CharField(max_length=20)
    nasdaq_tickers = models.CharField(max_length=20)
    nyse_tickers = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = "list_of_tickers_lt_10B"


class ListOfTickersLt1B(models.Model):
    id = models.AutoField(primary_key=True)
    ticker = models.CharField(max_length=20)
    market_cap = models.FloatField()
    nasdaq_tickers = models.BooleanField()
    nyse_tickers = models.BooleanField()

    class Meta:
        managed = False
        db_table = "list_of_tickers_lt_1B"


class ListOfTickersLt5B(models.Model):
    id = models.AutoField(primary_key=True)
    ticker = models.CharField(max_length=20)
    nasdaq_tickers = models.BooleanField()
    nyse_tickers = models.BooleanField()

    class Meta:
        managed = False
        db_table = "list_of_tickers_lt_5B"


class MarketBreadth(models.Model):
    date = models.DateField()
    ma50_number_of_stocks_above = models.IntegerField(blank=True, null=True)
    ma50_number_of_stocks_below = models.IntegerField(blank=True, null=True)
    ma50_pct_of_stocks_above = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    ma100_number_of_stocks_above = models.IntegerField(blank=True, null=True)
    ma100_number_of_stocks_below = models.IntegerField(blank=True, null=True)
    ma100_pct_of_stocks_above = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    ma200_number_of_stocks_above = models.IntegerField(blank=True, null=True)
    ma200_number_of_stocks_below = models.IntegerField(blank=True, null=True)
    ma200_pct_of_stocks_above = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "market_breadth"


class Momentum123(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "momentum_12_3"


class Momentum62(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "momentum_6_2"


class MonthlyChange(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    one_month_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    three_months_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    six_months_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    twelve_months_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    two_months_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "monthly_change"


class November05Best(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "november05_best"


class November05Worst(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "november05_worst"


class StockData(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    close = models.FloatField()
    high = models.FloatField()
    low = models.FloatField()
    open = models.FloatField()
    volume = models.BigIntegerField()
    ytd = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    august05 = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    ma50 = models.FloatField(blank=True, null=True)
    ma50_above = models.BooleanField(blank=True, null=True)
    ma100 = models.FloatField(blank=True, null=True)
    ma100_above = models.BooleanField(blank=True, null=True)
    ma200 = models.FloatField(blank=True, null=True)
    ma200_above = models.BooleanField(blank=True, null=True)
    november05 = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )
    weekly_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "stock_data"


class WeeklyChangeBest(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "weekly_change_best"


class WeeklyChangeWorst(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "weekly_change_worst"


class YtdBest(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "ytd_best"


class YtdWorst(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.DateField()
    ticker = models.CharField(max_length=20)
    pct_change = models.DecimalField(
        max_digits=10, decimal_places=2, blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "ytd_worst"
