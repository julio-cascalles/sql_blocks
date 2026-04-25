SELECT
        p.name,
        CASE
            WHEN p.age BETWEEN 0 AND 13 THEN
                CASE
                    WHEN gender = 'M' THEN 'boy'
                    WHEN gender = 'F' THEN 'girl'
                END
            WHEN p.age BETWEEN 14 AND 20 THEN
                CASE
                    WHEN gender = 'M' THEN 'young man'
                    WHEN gender = 'F' THEN 'young woman'
                END
                WHEN p.age BETWEEN 21 AND 40 THEN
                CASE
                    WHEN gender = 'M' THEN 'man'
                    WHEN gender = 'F' THEN 'woman'
                END
            WHEN p.age BETWEEN 41 AND 99 THEN
                CASE
                    WHEN gender = 'M' THEN 'old man'
                    WHEN gender = 'F' THEN 'old woman'
                END
            END AS ptype
FROM
        Person p
