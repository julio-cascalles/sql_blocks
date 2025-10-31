SELECT u001.name, agg_vendas.total
FROM (
      SELECT id, seller_name name FROM Sales WHERE status = 'active'
      UNION
      SELECT id, emp_name as name FROM Employee WHERE depto = 35 /* Internal sales */
) AS u001
JOIN (
      SELECT user_id as id, Sum(sale_value) as total from Order group by user_id
      UNION ALL
      SELECT person as id, Sum(price) as total FROM Ecommerce WHERE sale_type = 'internal'
) as agg_vendas
ON u001.id = agg_vendas.user_id
