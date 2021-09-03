SELECT
    ticker
FROM
    tickers
WHERE
    (
        EXTRACT(
            YEAR
            FROM
                lastpricedate
        ) - EXTRACT(
            YEAR
            FROM
                firstpricedate
        ) >= 5
        AND EXTRACT(
            YEAR
            FROM
                lastquarter
        ) - EXTRACT(
            YEAR
            FROM
                firstquarter
        ) >= 5
        AND category = 'Domestic Common Stock'
        AND currency = 'USD'
    );