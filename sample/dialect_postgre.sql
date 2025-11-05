        SELECT
            Substring(EAN, '\d+',),
            Date_Part(ref_date, YEAR) as ref_year,
            Current_date() - due_date as elapsed_time
        FROM
            Orders
        WHERE 
            customer_id = 35
