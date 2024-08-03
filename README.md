# SQL_Blocks

### 1 - You can assemble a simple object that will then be converted into an SQL command:

> a = Select('Actor') # --> SELECT * FROM Actor act

_Note that an alias "act" has been added._

You can specify your own alias:  `a = Select('Actor a')`

---
### 2 - You can also add a field, like this...

* a = Select('Actor a', **name=Field**)

* Here are another ways to add a field:
    - Select('Actor a', name=Distinct )

    - Select('Actor a', name=NamedField('actors_name'))

    - Select(
        'Actor a', 
        name=NamedField('actors_name', Distinct)
    )

---

### 3 - To set conditions, use **Where**:
* For example, `a = Select(... age=Where.gt(45) )`

    Some possible conditions:
    * field=Where.eq(value) - ...the field is EQUAL to the value;
    * field=Where.gt(value) - ...the field is GREATER than the value;
    * field=Where.lt(value) - ...the field is LESS than the value;

    3.1 -- If you want to filter the field on a range of values:
    
    `a = Select( 'Actor a', age=Between(45, 69) )`

    3.2 -- Sub-queries:
```
query = Select('Movie m', title=Field,
    id=SelectIN(
        'Review r',
        rate=Where.gt(4.5),
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
        genre=Where.eq("Sci-Fi"),
        awards=Where.like("Oscar")
    )
```
> Could be AND=Options(...)

3.4 -- Negative conditions use the _Not_ class instead of _Where_
```
based_on_book=Not.is_null()
```

3.5 -- List of values
```
hash_tag=Where.list(['space', 'monster', 'gore'])
```

---
### 4 - A field can be two things at the same time:

* m = Select('Movie m' release_date=[Field, OrderBy])
    - This means that the field will appear in the results and also that the query will be ordered by that field.
* Applying **GROUP BY** to item 3.2, it would look like this:
    ```    
    SelectIN(
        'Review r', movie=[GroupBy, Distinct],
        rate=Having.avg(Where.gt(4.5))
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

### 9 - Comparing objects

9.1
```
        a1 = Select.parse('''
                SELECT gender, max(age) FROM Actor act
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
    rate=[GroupBy, Having.avg(Where.gt(4.5))]
)
m1 = Select(
    Movie=Table('title,release_date),
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
