SELECT
    s.product,
    s.region, /* north south east west */
    s.month_ref /* 1=jan 2=feb 3=mar 4=apr */
FROM
    Sales s
GROUP BY
    s.product
