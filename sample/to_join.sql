SELECT
    o.ref_date, o.quantity
FROM
    Order(person_id) -> person(id)
order by
    o.ref_date DESC
