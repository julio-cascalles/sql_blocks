# SQL_Blocks

## _SQL_Blocks_ is useful for building complex SQL commands through smaller query blocks:

---

### 1 - You can assemble a simple object that will then be converted into an SQL command:

> a = Select('Actor') # --> SELECT * FROM Actor act

_Note that an alias "act" has been added._

You can specify your own alias:  `a = Select('Actor a')`

---
### 2 - You can also add a field, contains this...

* a = Select('Actor a', **name=Field**)

* Here are another ways to add a field:
    - Select('Actor a', name=Distinct )

    - Select('Actor a', name=NamedField('actors_name'))

    - Select(
        'Actor a', 
        name=NamedField('actors_name', Distinct)
    )


    2.1 -- Using expression as a field:
```
    Select(
        'Product',
        due_date=NamedField(
            'YEAR_ref',
            ExpressionField('extract(year from {f})') #  <<---
        )
    )
```
...should return: 
**SELECT extract(year from due_date) as YEAR_ref...**

Possible tags in ExpressionField:
* {f} - The field name;
* {af} - The field name preceded by the table alias;
    > Can be written as {a.f} or %
* {t} - The table name;
* {a} - Only the table alias.
---

### 3 - To set conditions, use **Where**:
* For example, `a = Select(... age=gt(45) )`

    Some possible conditions:
    * field=eq(value) - ...the field is EQUAL to the value;
    * field=gt(value) - ...the field is GREATER than the value;
    * field=lt(value) - ...the field is LESS than the value;

> You may use Where.**eq**, Where.**gt**, Where.**lt** ... or simply **eq**, **gt**, **lt** ... 😉

3.1 -- If you want to filter the field on a range of values:

`a = Select( 'Actor a', age=Between(45, 69) )`
...but if it is a time slot within the same day, you can do it like this:
`Select(..., event_date=SameDay("2024-10-03"))`
This results in
```
    SELECT ...
    WHERE
        event_date >= '2024-10-03 00:00:00' AND
        event_date <= '2024-10-03 23:59:59'
```
---

3.2 -- Sub-queries:
```
query = Select('Movie m', title=Field,
    id=SelectIN(
        'Review r',
        rate=gt(4.5),
        movie_id=Distinct
    )
)
```

**>> print(query)** 

        SELECT
            m.title
        FROM
            Movie m
        WHERE
            m.id IN (
                SELECT DISTINCT r.movie
                FROM Review r WHERE r.rate > 4.5
            )
    
3.3 -- Optional conditions:
```
    OR=Options(
        genre=eq("Sci-Fi"),
        awards=contains("Oscar")
    )
    AND=Options(
        ..., name=startswith('Chris')
    )
```

3.4 -- Negative conditions use the _Not_ class instead of _Where_
```
based_on_book=Not.is_null()
```

3.5 -- List of values
```
hash_tag=inside(['space', 'monster', 'gore'])
```

3.6 -- Combining ExpressionField with Where condition:
* The **formula** method allows you to write an expression as a condition:
```
query=Select(
    'Folks f2',
    id=Where.formula('({af} = a.father OR {af} = a.mother)')
)
```
> Results: `WHERE...f2.id = a.father OR f2.id = a.mother`

---
### 4 - A field can be two things at the same time:

* m = Select('Movie m' release_date=[Field, OrderBy])
    - This means that the field will appear in the results and also that the query will be ordered by that field.
* Applying **GROUP BY** to item 3.2, it would look contains this:
    ```    
    SelectIN(
        'Review r', movie=[GroupBy, Distinct],
        rate=Having.avg(gt(4.5))
    )
    ```
---
### 5 - Relationships:
```
    query = Select('Actor a', name=Field,
        cast=Select('Cast c', id=PrimaryKey)
    )
```
**>> print(query)**    
```
SELECT
    a.name
FROM
    Actor a
    JOIN Cast c ON (a.cast = c.id)    
```

---
**5.1  Multiple tables without JOIN**
> Warning: This is **NOT** recommended!  ⛔


#### Example:
    singer = Select(
        "Singer artist", id=PrimaryKey,
        name=NamedField('artist_name')
    )
    album = Select (
        "Album album",
        name=NamedField('album_name'),
        artist_id=Where.join(singer), #  <===== 👀
    )
**>> print(query)**    

    SELECT
            album.name as album_name,
            artist.name as artist_name,
            album.year_recorded
    FROM
            Album album
            ,Singer artist
    WHERE
            (album.artist_id = artist.id)

(*) --> For more than one relationship, use the **pairs** parameter.


---
### 6 - The reverse process (parse):
```
text = """
        SELECT
                cas.role,
                m.title,
                m.release_date,
                a.name as actors_name
        FROM
                Actor a
                LEFT JOIN Cast cas ON (a.cast = cas.id)
                LEFT JOIN Movie m ON (cas.movie = m.id)
        WHERE
                (
                    m.genre = 'Sci-Fi'
                    OR
                    m.awards LIKE '%Oscar%'
                )
                AND a.age <= 69 AND a.age >= 45
        ORDER BY
                m.release_date DESC
"""
```

`a, c, m = Select.parse(text)`

**6.1  --- print(a)**
```
    SELECT
            a.name as actors_name
    FROM
            Actor a
    WHERE
            a.age <= 69
            AND a.age >= 45
```

**6.2 --- print(c)**

    SELECT
            c.role
    FROM
            Cast c

**6.3 --- print(m)**

    SELECT
            m.title,
            m.release_date
    FROM
            Movie m
    WHERE
            ( m.genre = 'Sci-Fi' OR m.awards LIKE '%Oscar%' )
    ORDER BY
            m.release_date DESC



**6.4 --- print(a+c)**

    SELECT
            a.name as actors_name,
            cas.role
    FROM
            Actor a
            JOIN Cast cas ON (a.cast = cas.id)
    WHERE
            a.age >= 45
            AND a.age <= 69

**6.5 --- print(c+m)** 
> `... or  print(m+c)`

    SELECT
            cas.role,
            m.title,
            m.release_date,
            m.director
    FROM
            Cast cas
            JOIN Movie m ON (cas.movie = m.id)
    WHERE
            ( m.genre = 'Sci-Fi' OR m.awards LIKE '%Oscar%' )
            AND m.director LIKE '%Coppola%'
    ORDER BY
            m.release_date,
            m.director
---

### 7 - You can add or delete attributes directly in objects:
* a(gender=Field)
* m.delete('director')

---

### 8 - Defining relationship on separate objects:
```
a = Select...
c = Select...
m = Select...
```
`a + c => ERROR: "No relationship found between Actor and Cast"`

8.1 - But...

    a( cast=ForeignKey('Cast') )
    c(id=PrimaryKey)

**a + c => Ok!**

8.2

    c( movie=ForeignKey('Movie') )
    m(id=PrimaryKey)

> **c + m => Ok!**
>> **m + c => Ok!**


---
**8.3 Difference between queries**
```
    STATUS_DELIVERED_OK = 93
    orders = Select('orders',
        customer_id=ForeignKey('customers'),
        status=eq(STATUS_DELIVERED_OK)
    )
    customers = Select('customers'
        id=PrimaryKey, name=Field
    )
    gap = orders - customers
```
return _customers without orders_:

    SELECT
            c.name
    FROM
            customers c
    WHERE
            NOT c.id IN (
                SELECT o.customer_id FROM orders o
                WHERE o.status = 93
            )
---

### 9 - Comparing objects

9.1
```
        a1 = Select.parse('''
                SELECT gender, Max(act.age) FROM Actor act
                WHERE act.age <= 69 AND act.age >= 45
                GROUP BY gender
            ''')[0]

        a2 = Select('Actor',
            age=[ Between(45, 69), Max ],
            gender=[GroupBy, Field]
        )       
```
> **a1 == a2 # --- True!**



9.2
```
    m1 = Select.parse("""
        SELECT title, release_date FROM Movie m ORDER BY release_date 
        WHERE m.genre = 'Sci-Fi' AND m.awards LIKE '%Oscar%'
    """)[0]

    m2 = Select.parse("""
        SELECT release_date, title
        FROM Movie m
        WHERE m.awards LIKE '%Oscar%' AND m.genre = 'Sci-Fi'
        ORDER BY release_date 
    """)[0]
```

**m1 == m2  #  --- True!**


9.3
```
best_movies = SelectIN(
    Review=Table('role'),
    rate=[GroupBy, Having.avg(gt(4.5))]
)
m1 = Select(
    Movie=Table('title,release_date'),
    id=best_movies
)

sql = "SELECT rev.role FROM Review rev GROUP BY rev.rate HAVING Avg(rev.rate) > 4.5"
m2 = Select(
    'Movie', release_date=Field, title=Field,
    id=Where(f"IN ({sql})")
)
```
**m1 == m2 # --- True!**

---

### 10 - CASE...WHEN...THEN
    Select(
        'Product',
        label=Case('price').when(
            lt(50), 'cheap'
        ).when(
            gt(100), 'expensive'
        ).else_value(
            'normal'
        )
    )

* 10.1 - If the labels used in the CASE are based on ranges of values ​​in sequence, you can use the **Range class**:

        query = Select(
            'People p',
            age_group=Range('age',{  # <<---------- 
                'adult': 50,
                'teenager': 17,
                'child': 10,
                'elderly': 70,
                'young': 21,
            })
        )
is equivalent to...
```
        SELECT
                CASE
                    WHEN p.age BETWEEN 0 AND 10 THEN 'child'
                    WHEN p.age BETWEEN 11 AND 17 THEN 'teenager'
                    WHEN p.age BETWEEN 18 AND 21 THEN 'young'
                    WHEN p.age BETWEEN 22 AND 50 THEN 'adult'
                    WHEN p.age BETWEEN 51 AND 70 THEN 'elderly'
                END AS age_group
        FROM
                People p
```

* 10.2 `If` class

Usefull to conditional Sum, Avg, Count...

**Example:**

    Select('Loan', 
        penalty=If('days_late', Sum, gt(0))
    )
    # ...OR... penalty=If('days_late > 0', Sum)

results...
```
SELECT
        Sum(CASE
                WHEN days_late > 0 THEN penalty
                ELSE 0
        END)
FROM
        Emprestimo
```

---

### 11 - optimize method
    p1 = Select.parse("""
            SELECT * FROM Product p
            WHERE (p.category = 'Gizmo'
                    OR p.category = 'Gadget'
                    OR p.category = 'Doohickey')
                AND NOT price <= 387.64
                AND YEAR(last_sale) = 2024
            ORDER BY
                category
        """)[0]
        p1.optimize() #  <<===============
        p2 = Select.parse("""
            SELECT category FROM Product p
            WHERE category IN ('Gizmo','Gadget','Doohickey')
                and p.price > 387.64
                and p.last_sale >= '2024-01-01'
                and p.last_sale <= '2024-12-31'
            ORDER BY p.category LIMIT 100
        """)[0]
        p1 == p2 # --- True!

 This will...
* Replace `OR` conditions to `SELECT IN ...`
* Put `LIMIT` if no fields or conditions defined;
* Normalizes inverted conditions;
* Auto includes fields present in `ORDER/GROUP BY`;
* Replace `YEAR` function with date range comparison.

> The method allows you to select which rules you want to apply in the optimization...Or define your own rules!

>> NOTE: When a joined table is used only as a filter, it is possible that it can be changed to a sub-query:

    query = Select(
        'Installments i', due_date=Field,  customer=Select(
            'Customer c', id=PrimaryKey,
            name=endswith('Smith')
        )
    )
    print(query)
    print('-----')
    query.optimize([RuleReplaceJoinBySubselect])
    print(query)
```
SELECT
        i.due_date
FROM
        Installments i
        JOIN Customer c ON (i.customer = c.id)
WHERE
        c.name LIKE '%Smith'
-----
SELECT
        i.due_date
FROM
        Installments i
WHERE
        i.customer IN (SELECT c.id FROM Customer c WHERE c.name LIKE '%Smith')
```

---

### 12 - Adding multiple fields at once
```
    query = Select('post p')
    query.add_fields(
        'user_id, created_at',
        [OrderBy, GroupBy]
    )
```
...is the same as...
```
    query = Select(
        'post p',
        user_id=[Field, GroupBy, OrderBy],
        created_at=[Field, GroupBy, OrderBy]
    )
```

### 13 - Change parser engine
```
a, c, m = Select.parse(
    """
        Actor(name, id ?age = 40)
        <- Cast(actor_id, movie_id) ->
        Movie(id ^title)
    """,
    CypherParser
    # ^^^ recognizes syntax like Neo4J queries
)
```

**print(a+c+m)**
```
SELECT
        act.name,
        mov.title
FROM
        Cast cas
        JOIN Movie mov ON (cas.movie_id = mov.id)
        JOIN Actor act ON (cas.actor_id = act.id)
WHERE
        act.age = 40
ORDER BY
        mov.title
```
---
> **Separators and meaning:**
* `(  )`  Delimits a table and its fields
* `,` Separate fields
* `?` For simple conditions (> < = <>)
* `<-` connects to the table on the left
* `->` connects to the table on the right
* `^` Put the field in the ORDER BY clause
* `@` Immediately after the table name, it indicates the grouping field.
* `$` For SQL functions like **avg**$_field_, **sum**$_field_, **count**$_field_...
* `*` Sets the primary key field.


---
## `detect` function

It is useful to write a query in a few lines, without specifying the script type (cypher, mongoDB, SQL, Neo4J...)
### Examples:

> **13.1 - Relationship**
```
query = detect(
    'MATCH(c:Customer)<-[:Order]->(p:Product)RETURN c, p'
)
print(query)
```
##### output:
    SELECT * FROM
        Order ord
        LEFT JOIN Customer cus ON (ord.customer_id = cus.id)
        RIGHT JOIN Product pro ON (ord.product_id = pro.id)
> **13.2 - Grouping**
```
query = detect(
    'People@gender(avg$age?region="SOUTH"^count$qtde)'
)
print(query)
```
##### output:
    SELECT
            peo.gender,
            Avg(peo.age),
            Count(*) as qtde
    FROM
            People peo
    WHERE
            peo.region = "SOUTH"
    GROUP BY
            peo.gender
    ORDER BY
            peo.qtde

> **13.3 - Many conditions...**
```
    print( detect('''
        db.people.find({
            {
                $or: [
                    {status:{$eq:"B"}},
                    age:{$lt:50}
                ]
            },
            age:{$gte:18},  status:{$eq:"A"}
        },{
            name: 1, user_id: 1
        }).sort({
            user_id: -1
        })
    ''') )
```
#### output:
    SELECT
            peo.name,
            peo.user_id
    FROM
            people peo
    WHERE
            ( peo. = 'B' OR peo.age < 50 ) AND
            peo.age >= 18 AND
            peo.status = 'A'
    ORDER BY
            peo.user_id DESC

> **13.4 - Relations with same table twice (or more)**

Automatically assigns aliases to each side of the relationship (In this example, one user invites another to add to their contact list)
```
    print( detect(
        'User(^name,id) <-Contact(requester,guest)-> User(id,name)'
       # ^^^ u1                                        ^^^ u2
    ) )
```
    SELECT
            u1.name,
            u2.name
    FROM
            Contact con
            RIGHT JOIN User u2 ON (con.guest = u2.id)
            LEFT JOIN User u1 ON (con.requester = u1.id)
    ORDER BY
            u1.name

---
### `translate_to` method
From a Select object, it returns the text to a script in any of the languages ​​below:
* QueryLanguage - default
* MongoDBLanguage
* Neo4JLanguage
* DatabricksLanguage
* PandasLanguage

---
### 14 - Window Function

Aggregation functions (Avg, Min, Max, Sum, Count) -- or Window functions (Lead, Lag, Row_Number, Rank) -- have the **over** method...

    query=Select(
        'Enrollment e',
        payment=Sum().over(
            student_id=Partition, due_date=OrderBy,
            # _=Rows(Current(), Following(5)), 
               # ^^^-------> ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING
            # _=Rows(Preceding(3), Following()),
               # ^^^-------> ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING
            # _=Rows(Preceding(3)) 
               # ^^^-------> ROWS 3 PRECEDING
        ).As('sum_per_student')
    )

...that generates the following query:

```
SELECT
        Sum(e.payment) OVER(
                PARTITION BY student_id
                ORDER BY due_date
        ) as sum_per_student
FROM
        Enrollment e
```
---
### 15 - The `As` method:
    query=Select(
        'Customers c',
        phone=[
            Not.is_null(),
            SubString(1, 4).As('area_code', GroupBy)
        ],
        customer_id=[
            Count().As('customer_count', OrderBy),
            Having.count(gt(5))
        ]
    )
You can use the result of a function as a new field -- and optionally use it in ORDER BY and/or GROUP BY clause(s):
```
SELECT
        SubString(c.phone, 1, 4) as area_code,
        Count(c.customer_id) as customer_count
FROM
        Customers c
WHERE
        NOT c.phone IS NULL
GROUP BY
        area_code HAVING Count(c.customer_id) > 5
ORDER BY
        customer_count
```
---
### 16 - Function classes
You may use this functions:
* SubString
* Round
* DateDiff
* Year
* Current_Date
* Avg
* Min
* Max
* Sum
* Count
* Lag
* Lead
* Row_Number
* Rank
* Coalesce
* Cast
> Some of these functions may vary in syntax depending on the database.
For example, if your query is going to run on Oracle, do the following:

`Function.dialect = Dialect.ORACLE`


>  Most of this functions you can use nested inside each other.
*Example:*
```
    Select(...
        event_date=Substring(
            Cast("CHAR"), 12, 19 
        ).As('time')
    )
```
Results...
```
    SELECT ...
    SubString(Cast(event_date As char), 12, 19) as time
```

>> `Function.auto_convert` option (default: True)

- Put Cast(...) when there is a difference between the types of the parameter and the return of the nested function
```
birth=Round( DateDiff(Current_Date()) ).As('age')
```
...Returns...
```
SELECT
    Round(
        Cast(Current_Date() - p.birth As FLOAT)
       /* ^^^  */
    ) as age
...
```
---

### 16.1 - _GroupBy as instance_

Another way to use GroupBy is to pass functions as parameters:

```
    Function.dialect = Dialect.ORACLE
    query = Select(
        'Sales s',
        ref_date=GroupBy(
            ref_year=Year, qty_sold=Sum('quantity'),
            vendor=Select(
                'Vendor v',
                id=[PrimaryKey, Field], name=Field
            )
        )
    )
    print(query)
```
results..
```
SELECT
        Extract(Year FROM s.ref_date) as ref_year,
        Sum(quantity) as qty_sold,
        v.id,
        v.name
FROM
        Sales s
        JOIN Vendor v ON (s.vendor = v.id)
GROUP BY
        ref_year,
        v.id,
        v.name
```

---
### 17 - CTE and Recursive classes

* **17.1 - _CTE class_**
```
    query = Select(
        'SocialMedia s', post=Count, reaction=Sum, user=GroupBy
    )
    print( CTE('Metrics', [query]) )
```
The result is...
```
    WITH Metrics AS (
            SELECT Count(s.post), Sum(s.reaction) FROM SocialMedia s GROUP BY user
    )SELECT * FROM Metrics
```

* **17.2 - _Recursive class_**
```
q1 = Select(
    'SocialMedia me', name=[ eq(MY_NAME), Field ]
)
q2 = Select(
    'SocialMedia you' name=Field, id=Where.formula('{af} = n.friend')
)
print( Recursive('Network', [q1, q2]) )
```
The result is...
```
WITH RECURSIVE Network AS (
        SELECT me.name FROM SocialMedia me WHERE 
        me.name = 'Júlio Cascalles'
UNION ALL
        SELECT you.name FROM SocialMedia you , Network n
        WHERE  you.id = n.friend
)SELECT * FROM Network
```

* **17.2.1 - The `create` method** ... parameters :
    - name: The name of the CTE
    - pattern: A cypher script that defines the tables used
    - formula: The format for `Where.formula` method _(*)_
    - init_value: The value for the condition in the first table
    - format (optional): If tables are files or internet hiperlinks, you may especify the extension and/or folder...
> Example: 
```
    R = Recursive.create(
        'Route R', 'Flyght(departure, arrival)',
        '[2] = R.[1]',  'JFK',  format='.csv'
    ) #                  ^^^--- Flyghts from JFK airport
```
_...Creates a recursive CTE called Route, using Flyght table, where the recursivity condition is Flyght.arrival equals to Route.departure_
>> (*) -- Note that [1] and [2] refers to first field and second field. 😉

Result:

    WITH RECURSIVE Route AS (
            SELECT f1.departure, f1.arrival
            FROM Flyght.csv f1
            WHERE f1.departure = 'JFK'
    UNION ALL
            SELECT f2.departure, f2.arrival
            FROM Flyght.csv f2
            , Route R
            WHERE  f2.arrival = R.departure
    )SELECT * FROM Route R

**17.2.2 - The `join` method** 

In the previous example, if you add this code...
`R.join('Airport(*id,name)', 'departure, arrival', format='.csv')`

...The result would be:

    WITH RECURSIVE Route AS (
        SELECT f1.departure, f1.arrival
        FROM Flyght.csv f1
        WHERE f1.departure = 'JFK'
    UNION ALL
        SELECT f2.departure, f2.arrival
        FROM Flyght.csv f2
        , Route R
        WHERE  f2.arrival = R.departure
    )SELECT
        a1.name, a2.name
    FROM
        Route R
        JOIN Airport.csv a2 ON (R.arrival = a2.id)
        JOIN Airport.csv a1 ON (R.departure = a1.id)


**17.2.3 - The `counter` method** 
Adds an increment field in queries inside CTE:
> Examples:
* `R.counter('stops', 0)` # -- counter starts with 0 and increment +1
* `R2.counter('generation', 5, '- 1')` # -- for the code below...
```
R2 = Recursive.create(
    'Ancestors a', 'People(id,name,father,mother,birth)',
    '(% = a.father OR % = a.mother)', 32630, '.parquet'
)
```
...Results:

    WITH RECURSIVE Ancestors AS (
        SELECT p1.id, p1.name, p1.father, p1.mother, p1.birth,
        5 AS generation  /* <<---- Most current generation ------------*/
        FROM People.parquet p1 WHERE p1.id = 32630
    UNION ALL
        SELECT p2.id, p2.name, p2.father, p2.mother, p2.birth,
        (generation- 1) AS generation /* <<-- Previous generation -----*/
        FROM People.parquet p2 , Ancestors a WHERE  (p2.id = a.father OR p2.id = a.mother)
    )SELECT * FROM Ancestors a


>> Note: Comments added later.
---

### CTEFactory class
CTEFactory exchanges subqueries for CTEs, simply by passing the text of the "dirty" query:

*Example*:
```
print(
        CTEFactory("""
            SELECT u001.name, agg_sales.total
            FROM (
                SELECT * FROM Users u
                WHERE u.status = 'active'
            ) AS u001
            JOIN (
                SELECT s.user_id, Sum(s.value) as total
                FROM Sales s
                GROUP BY s.user_id
            )
            As agg_sales
            ON u001.id = agg_sales.user_id
            ORDER BY u001.name
        """)        
)
```
results...
```
    WITH u001 AS (
        SELECT * FROM Users u
        WHERE u.status = 'active'
    ),
    WITH agg_sales AS (
        SELECT s.user_id, Sum(s.value) as total
        FROM Sales s
        GROUP BY s.user_id
    )
    SELECT
            u001.name,
            agg_sales.total
    FROM
            u001 u001
            JOIN agg_sales agg_sales ON
            (u001.id = agg_sales.user_id)
    ORDER BY
            u001.name
```
---
