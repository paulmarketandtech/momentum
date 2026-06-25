"""
WORKFLOW:
0. Once a month (at the end) dowload manually spreadsheet from website https://www.nasdaq.com/market-activity/stocks/screener
  - create a subfolder where to store those files
  - filter MC > $100M (number is still open)
    - populate table 'all_tickers' (?)
  - setup crontab to run it every first of the month (?)

1. Every Sunday run a script which reads all tickers from table 'all_tickers'
and dowloads MC from yfinance and updates the existing one.
"""

"""
File structure and funcions in it
monthly_update_from_file.py 
* filter_above_given_MC(MC, filename) -> df (?) 
    - reads filename, filters above MC and returns df 
* update_DB_with_tickers_and_MC(df) -> void (?)
    - sort df via MC (?)
    - delete everything from a table 
    - update with new values


yf_weekly_tickers_update.py 
(happens only on Sundays)
* create_list_of_tickers() -> df 
    - reads data from DB, creates and returns df 
* update_MC_from_YF(df) -> void 
    - loops through all tickers and dowloads MC from YF 
"""

"""
Old workflow:
once a week download csv with all tickers,
based on that, create list of tickers > $2B -> populate DB/list_of_tickers_lt_2B,
based on that, create list of tickers > $5B -> populate DB/list_of_tickers_lt_5B,

New temp workflow:
once a month download csv with all tickers,
based on that populate DB/list_of_all_tickers with those above $100M (probably will be $200M),
Once a week, on Sunday update MC from YF
based on that, create list of tickers > $2B -> populate DB/list_of_tickers_lt_2B,
based on that, create list of tickers > $5B -> populate DB/list_of_tickers_lt_5B,

Desired workflow:
similar to temp but have to decrease those $2B and $5B
"""

"""
TODO now.
monthly update is probably almost done so update weekly update so it will download data from YF
"""

"""
TODO on PROD 
create paths: weekly update -> monthly update 
update .env with those paths 
"""
