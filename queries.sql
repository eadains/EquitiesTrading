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