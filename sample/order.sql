SELECT
    o.ref_date,
    o.order_num,
    o.store,
    o.customer_id,
    o.product_id,
    o.price,
    o.discount
FROM
    Order o
WHERE
    o.store IN (27, 18, 49, 36)
    AND 
    o.price > 500
ORDER BY
    o.ref_date
