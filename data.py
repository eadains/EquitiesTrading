import psycopg2 as pg
import pandas as pd
from config import DATABASE_URI

idx = pd.IndexSlice


def get_prices():
    """
    Gets end of month adjusted close prices for every ticker in prices table
    """
    sql = """
        SELECT
            date,
            ticker,
            closeadj,
            close,
            volume
        FROM
            prices
        WHERE
            date IN (
                SELECT
                    MAX(date)
                FROM
                    prices
                WHERE
                    frequency = 'DAILY'
                GROUP BY
                    EXTRACT(
                        QUARTER
                        FROM
                            date
                    ),
                    EXTRACT(
                        YEAR
                        FROM
                            date
                    )
            )
        ORDER BY
            date ASC;
    """

    with pg.connect(DATABASE_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

    prices = pd.DataFrame.from_records(
        results,
        columns=["date", "ticker", "closeadj", "close", "volume"],
        coerce_float=True,
    )
    # There aren't duplicates here usually, but drop them just to be sure
    prices = prices.drop_duplicates(subset=["date", "ticker"])
    prices = prices.set_index(["date", "ticker"], verify_integrity=True)
    return prices


def do_reindex(df, prices):
    """
    This is some really convoluted shit to reindex the fundamentals dataframe
    to end of month like the prices dataframe while forward filling fundmental information
    from the last quarterly filing.
    Pandas multiindex is just not designed to do what I want, so this is what has to be done
    """
    # Turn ticker index into column
    df = df.reset_index(level=1)
    # For each ticker, reindex according to prices dataframe date index, limit forward filling to 12 months
    # to account for some companies not regularly filing quarterly reports
    df = df.groupby("ticker").apply(
        lambda x: x.reindex(index=prices.index.levels[0], method="ffill", limit=12)
    )
    # Tickers column gets duplicated in the index, so drop it
    df = df.drop(columns=["ticker"])
    # Index order gets reversed, so un-reverse it and sort
    return df.reorder_levels(["date", "ticker"]).sort_index()


def get_fundamentals(columns, prices):
    """
    Gets as reported quarterly fundamental data given prices dataframe from above function
    and a list of columns we want from the fundamentals table
    """
    sql = f"""
        SELECT
            datekey,
            ticker,
            {', '.join(columns)}
        FROM
            fundamentals
        WHERE
            dimension = 'ARQ'
        ORDER BY
            datekey ASC;
    """

    with pg.connect(DATABASE_URI) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

    fundamentals = pd.DataFrame.from_records(
        results, columns=["datekey", "ticker"] + columns, coerce_float=True
    )
    # For some reason when fetching prices the column already has datetime type,
    # but it doesn't here, so we have to manually convert it
    fundamentals["datekey"] = pd.to_datetime(fundamentals["datekey"])
    # Drop duplicates for setting index. There are some here for reasons that are beyond me.
    fundamentals = fundamentals.drop_duplicates(subset=["datekey", "ticker"])
    fundamentals = fundamentals.set_index(["datekey", "ticker"], verify_integrity=True)
    fundamentals = do_reindex(fundamentals, prices)
    return fundamentals


def filter_fundamentals(df):
    """
    Filter fundamental data for sensibility. REQUIRES 'sharefactor'
    AND 'sharesbas' COLUMNS
    - Market cap > 10,000,000
    - 250 > Price > 5
    - Share factor == 1
    """
    df["market_cap"] = df["sharesbas"] * df["close"]
    df = df[df["sharefactor"] == 1]
    df = df[df["close"].between(5, 250)]
    df = df[df["market_cap"] > 10000000]
    return df.dropna()


def get_data(columns):
    """
    Gets prices and fundamentals and joins them
    """
    prices = get_prices()
    fundamentals = get_fundamentals(columns, prices)
    return fundamentals.join(prices)
