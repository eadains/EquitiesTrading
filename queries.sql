SELECT
    datekey,
    ticker,
    pe
FROM
    fundamentals
WHERE
    dimension = 'ARQ'
ORDER BY
    date DESC;

SELECT
    date,
    ticker,
    closeadj
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
                MONTH
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

SELECT
    *
FROM
    prices
WHERE
    ticker IN (
        SELECT
            *
        FROM
            tickers
        WHERE
            (
                category = 'ETF'
                AND isdelisted = FALSE
                AND firstpricedate <= '2007-12-31' :: date
            )
    );