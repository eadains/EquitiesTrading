from typing import Type
import psycopg2 as pg
import pandas as pd
import numpy as np

from config import DATABASE_URI

idx = pd.IndexSlice


def get_market_returns():
    """
    Fetches returns for market proxy ETF, SPY

    Returns pandas Series
    """
    sql = """
        SELECT
            date,
            closeadj
        FROM
            prices
        WHERE
            ticker = 'SPY';
    """

    with pg.connect(DATABASE_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

    market_df = pd.Series(dict(results), dtype="float").sort_index()
    market_df = market_df.pct_change().dropna()
    return market_df


def get_prices_df(ticker):
    """
    Gets date, closeadj, close, and volume columns from database
    given specified ticker

    Inputs:
        ticker: string
    Outputs:
        pandas dataframe with datetime index
    """
    sql = f"""
        SELECT
            date,
            closeadj,
            close,
            volume
        FROM
            prices
        WHERE
            ticker = '{ticker}';
    """

    with pg.connect(DATABASE_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

    prices_df = pd.DataFrame.from_records(
        results, columns=["date", "closeadj", "close", "volume"], coerce_float=True
    )
    # Data consistency isn't 100%, so drop duplicated date rows
    prices_df = prices_df.drop_duplicates(subset=["date"])
    prices_df = prices_df.set_index("date", verify_integrity=True).sort_index()
    # Sometimes columns are totally empty so pandas fills with None but we actually want NaN
    # for math
    prices_df = prices_df.replace([None], np.nan)
    return prices_df


def get_fundamentals_df(ticker):
    """
    Gets fundamental data for specified ticker

    Inputs:
        ticker: string
    Outputs:
        pandas dataframe with datetime index
    """
    sql = f"""
        SELECT
            datekey,
            sharesbas,
            pb,
            assetsc,
            cashneq,
            liabilitiesc,
            depamor,
            roa,
            assets,
            divyield,
            debt,
            revenue
        FROM
            fundamentals
        WHERE
            (
                ticker = '{ticker}'
                AND dimension = 'ART'
            );
    """

    with pg.connect(DATABASE_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

    fund_df = pd.DataFrame.from_records(
        results,
        columns=[
            "date",
            "shares",
            "pb",
            "assetsc",
            "cash",
            "liabilitiesc",
            "deprec",
            "roa",
            "assets",
            "divyield",
            "debt",
            "revenue",
        ],
        coerce_float=True,
    )
    fund_df["date"] = pd.to_datetime(fund_df["date"])
    # Data consistency isn't 100%, so drop duplicated date rows
    fund_df = fund_df.drop_duplicates(subset=["date"])
    fund_df = fund_df.set_index("date", verify_integrity=True).sort_index()
    fund_df = fund_df.replace([None], np.nan)
    return fund_df


def calc_beta(stock_df, market_df):
    """
    Calculates beta from daily stock returns.

    Inputs:
    stock_df: stock returns
    market_df: market index RETURNS
    """
    # Try to get market returns for same days as stock returns, if they don't match up, catch index KeyError
    # and return NaN
    try:
        market_df = market_df.loc[stock_df.index]
    except KeyError:
        print("Market returns do not exist for some days in stock data. Skipping")
        return np.nan
    beta = np.sum(
        (stock_df - np.mean(stock_df)) * (market_df - np.mean(market_df))
    ) / np.sum((market_df - np.mean(market_df)) ** 2)
    return beta


def make_features(df, market_df):
    """
    Makes features matching "The Cross Section of Expected Stock Returns'
    https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2511246

    Inputs:
        df: pandas dataframe containing daily market and fundamental data
        market_df: pandas series containing daily index returns
    Outputs:
        pandas dataframe with end-of-month datetime index
    """
    result = {"date": df.index[-1]}
    # log of market capitalization
    result["logsize"] = np.log(df.iloc[-1]["close"] * df.iloc[-1]["shares"])
    # price / book ratio
    result["pb"] = df.iloc[-1]["pb"]
    # -12 month to -2 month stock return
    result["momentum"] = (df.iloc[-1]["closeadj"] / df.iloc[-43]["closeadj"]) - 1
    # log growth in outstanding shares
    result["issuance"] = np.log(df.iloc[-1]["shares"]) - np.log(df.iloc[0]["shares"])
    # log growth in non-cash working capital minus depreciation
    result["accruals"] = (
        (df.iloc[-1]["assetsc"] - df.iloc[-1]["cash"] - df.iloc[-1]["liabilitiesc"])
        / (df.iloc[0]["assetsc"] - df.iloc[0]["cash"] - df.iloc[0]["liabilitiesc"])
    ) - 1
    # return on assets
    result["roa"] = df.iloc[-1]["roa"]
    # log growth in assets
    result["assets"] = np.log(df.iloc[-1]["assets"]) - np.log(df.iloc[0]["assets"])
    # dividend yield
    result["divyield"] = df.iloc[-1]["divyield"]
    # beta
    returns = df["closeadj"].pct_change().dropna()
    result["beta"] = calc_beta(returns, market_df)
    # standard deviation of daily returns
    result["stddev"] = np.std(returns)
    # average daily share turnover
    result["turnover"] = np.mean(df["volume"] / df["shares"])
    # debt / market cap
    result["debt_price"] = df.iloc[-1]["debt"] / (
        df.iloc[-1]["close"] * df.iloc[-1]["shares"]
    )
    # sales / market cap
    result["sales_price"] = df.iloc[-1]["revenue"] / (
        df.iloc[-1]["close"] * df.iloc[-1]["shares"]
    )
    result["monthly_ret"] = (df.iloc[-1]["closeadj"] / df.iloc[-22]["closeadj"]) - 1
    return result


def create_feature_df(ticker):
    """
    Creates full feature dataframe for specified ticker

    Inputs:
        ticker: string
    """
    # Get all relevant data
    market_returns = get_market_returns()
    prices_df = get_prices_df(ticker)
    fund_df = get_fundamentals_df(ticker)

    # Reindex fundamentals dataframe to match prices dataframe,
    # limit fill to 252 days just in case there are gaps in financial statements
    df = fund_df.reindex(index=prices_df.index, method="ffill", limit=252)
    df = df.join(prices_df)

    # Create a range of end-of-month dates
    # Skip if index is size 0
    if df.index.size > 0:
        dates = pd.bdate_range(start=df.index[0], end=df.index[-1], freq="M")
    else:
        return None
    rolling_dfs = []

    # For each end of month date, select all data from the past year
    # If there isn't enough data, skip
    for date in dates:
        annual_df = df.loc[(date - pd.Timedelta(days=365)) : date]
        if len(annual_df) > 250:
            rolling_dfs.append(annual_df)

    # Make features for each rolling 12-month period
    # If rolling_dfs list is empty, there's not enough data so return None
    if rolling_dfs:
        df = pd.DataFrame([make_features(n, market_returns) for n in rolling_dfs])
    else:
        return None
    # Match each months data with the return at the end of the next month
    # for forecasting
    df["forward_ret"] = df["monthly_ret"].shift(-1)
    df["ticker"] = ticker
    df = df.dropna()
    # If dataframe has any rows, write to csv
    if len(df) > 1:
        df.to_csv(f"csv/{ticker}.csv", index=False)
        print(f"{ticker} done!")
    return None
