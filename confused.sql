SELECT
	s.value,
	s.quantity
FROM
	Sales s
	JOIN Product p ON (s.prod_id = p.id)
WHERE
	(s.store = 65 OR s.store = 31 or s.store = 84) AND 
	not p.name <> 'iPhone' AND 
	p.value * 1.05 < 1050
ORDER BY
	s.ref_date DESC
