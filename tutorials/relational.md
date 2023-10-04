# Relational Database

## Introduction

This tutorial is designed to get you started with our relational database and ready to complete the relational Tick. We'll be using a database of films, cast and crew information from the [Internet Movie Database (IMDb)](https://developer.imdb.com/non-commercial-datasets/). From here we'll use 'movie', instead of 'film', for consistency. The dataset from IMDb is not complete; it contains only ~10 people per movie, and only ~3 genres per movie -- but this will be enough for our purposes. It may mean that cast members you expect to see on a movie are missing, or links between movies you expect to find don't exist.

Our relation database management system (DBMS) will be [SQLite](https://www.sqlite.org/index.html), a popular open-source format chosen for its simplicity. Unlike other mainstream database systems, such as [PostgreSQL](https://www.postgresql.org/) or [MariaDB](https://mariadb.org/), it does not use a client-server model. SQLite uses a single file to store the database and does not support multiple concurrent users. SQLite is widely used in mobile operating systems; iOS and Android both use SQLite databases extensively under the hood. All your text messages, WhatsApp and Signal chats will be stored in SQLite databases on your device, for example.

Like other relational databases, SQLite uses [SQL](https://en.wikipedia.org/wiki/SQL) as the query language. SQL is an evolving language, vast in its scope. SQL is standardised, but many database vendors use their own variations in syntax and keywords. Whilst this tutorial will focus on SQLite, the syntax we will cover should be common to all SQL databases unless noted.

## Installation

### Getting SQLite

There are several ways to access an SQLite database. Any of these options will work fine for this course; you'll use the same SQL commands regardless of the interface.

#### Command line access 
The SQLite project provides binaries for command-line access to an SQLite file. You'll want the `sqlite-tools` for your OS, providing the `sqlite3` command. You can [download the latest binaries](https://sqlite.org/download.html), and read the [sqlite command documentation](https://sqlite.org/cli.html).

If you're on Linux, `apt install sqlite` will quickly install the SQLite library and command line tool. On MacOS, a version of SQLite is already installed as `/usr/bin/sqlite3`. On Windows, download the `sqlite-tools-win32-...` zip file from the link above and extract the `sqlite3.exe` executable.

To use the tool, run `sqlite3 [filename]` and you should see the SQLite prompt where you can type queries:
```
$ sqlite3 movies.sqlite
SQLite version 3.42.0
Enter ".help" for usage hints.
sqlite>
sqlite> SELECT 1;
1
```

(Make sure you use `sqlite3` and not `sqlite` if you have both installed; we're using SQLite version 3, the previous version is very old!).

This SQLite tool supports some meta-commands that are not SQL; these start with a `.` and are about managing the database system and not the data. You might want to run `.mode column` first of all, to change the output format to include column titles and pretty-print the results. There are other output modes (including `table`, `csv`, and even `json`) which you can explore. You may also want to choose a different value to represent `NULL`; by default whitespace is used, but you can change it using `.nullvalue`, e.g. `.nullvalue <null>` to use the string `<null>`. 

#### Graphical user interface

There are graphical tools for querying SQLite databases, including the popular [DB Browser for SQLite](https://sqlitebrowser.org/) and [JetBrain's DataGrip](https://www.jetbrains.com/datagrip/).

To open the database file with DB Browser for SQLite, just choose "Open Database" and find the `movies.sqlite` file. You can see an overview of the tables in the "Database Structure" tab, view the data in the tables in the "Browse Data" tab, and write SQL queries directly in the "Execute SQL" tab.

DataGrip is more complicated, but if you already use JetBrains tools then it may be an option for you.


#### Python

If you have Python installed, which you will need for the document databases part of the course, you can use the SQLite library contained in the Python standard library.

In a Python program, import `sqlite3` and connect to a database:
```
> import sqlite3
> con = sqlite3.connect("/path/to/movies.sqlite")
```
To execute queries and fetch the results of the query we have to use a cursor object:
```
> cur = con.cursor()
> cur.execute("SELECT 1")
> cur.fetchone()
(1,)
```
Note that we have to execute our statement and separately fetch the results. If you expect only one row of results, you can use `cur.fetchone()`; if you expect many rows, use `cur.fetchall()`. Each row is returned as a tuple, even rows with only one column. If you use `fetchall`, a list of these row tuples is returned. If there are no results, `fetchone` returns `None`, but `fetchall` returns an empty list.

Note that we didn't need to terminate our SQL query with a semi-colon; the `execute` function only runs one statement at once and so the terminating semi-colon is implied. This is in part a security feature to help reduce the risk of SQL injection. If you want to run several statements at once, use `cur.executescript(...)` instead.

Using the `sqlite3` library like this is much closer to how you would use a database in a real application, but it may be more difficult to experiment with SQL commands this way.


### Getting the data

Download the SQLite database [movies.sqlite](!!!!!) and save it somewhere sensible. This file contains our entire database. If you decide to play with the `INSERT`, `UPDATE` and `DELETE` SQL commands later on, perhaps download a fresh copy of the database before attempting the Tick.


## SQL by example

Our movies database has several tables:

 - movies
 - people
 - genres
 - has_genre   
 - has_position
 - plays_role
 - ratings

If we think about these in terms of the Entity-Relation model, the first three tables are Entities (`movies`, `people`, `genres`) and the next three are Relationships (`has_genre`, `has_position`, `plays_role`). Only `ratings` is a bit odd; hopefully as the course progresses you will understand why the `rating` entity and `has_rating` relationship have been merged into one table, but why we may not want to just put the rating data directly into the `movies` table.

### Table schemas

The `movies` table has these columns:

- `movie_id`: a unique identifier (a key, and the primary key) for each movie.
- `title`: the title of the movie.
- `year`: the release year for the movie.
- `type`: whether the movie is a `movie`, a `tvMovie`, or a direct-to-video `video` movie.
- `minutes`: the length of the movie in minutes.

The `people` table has these columns:

 - `person_id`: a unique identifier (a key, and the primary key) for each person.
 - `name`: the name of the person.
 - `birthyear`: the year the person was born, if known.
 - `deathyear`: the year the person died, if known. A `NULL` value may mean the person is still alive, or that their death year has not been recorded.

The `genres` table is simple:

 - `genre_id`: a unique identifier (a key, and the primary key) for each genre.
 - `name`: the name of the genre.

The `has_genre` table has only two columns, which together form a composite primary key:

 - `movie_id`: the ID of the movie, a foreign key for `movies.movie_id`.
 - `genre_id`: the ID of the genre, a foreign key for `genres.genre_id`.

The `has_position` table again has a composite key, this time with three of the four columns: `(person_id, movie_id, position)`:
 
- `person_id`: the ID of the person, a foreign key for `people.person_id`.
- `movie_id`: the ID of the movie, a foreign key for `movies.movie_id`.
- `position`: whether the person was an `actor`, `director`, `producer`, `writer` or `composer` for the movie.
- `job`: for some positions, extra details on the job they did. There are `NULL` values where positions do not have this extra detail, or where none was provided.

The `plays_role` table is similar, with a composite primary key formed of all three columns. Why are all three columns needed for the primary key? (Hint: some actors play multiple roles in the same movie).

 - `person_id`: the ID of the person, a foreign key for `people.person_id`.
 - `movie_id`: the ID of the movie, a foreign key for `movies.movie_id`.
 - `role`: the name of the role the actor played in that movie.

The `ratings` table has three columns, and contains IMDb rating data. It could be extended to allow ratings from other services like TMDB or Letterboxd; how might we do that?

 - `movie_id`: the ID of the movie, a foreign key for `movies.movie_id` and the primary key of the table.
 - `rating`: the average IMDb rating for the film.
 - `votes`: the number of ratings combined to form the average rating above.


### Comments

Before looking at some queries, it is worth noting that comments in SQL use either `--` or the C-style `/* ... */`. You may see these used in examples and find them useful to document your work.
```sql
-- this is a single-line comment
SELECT 1;
/* this 
   is
   a
   multi-line
   comment */
SELECT 2;
```

### Basic SELECT queries

To get data from a table, we can use the `SELECT ... FROM ...` construct. Since there are many rows in our database, whilst we are exploring the data it can be helpful to reduce the number of rows returned. We can do this by adding `LIMIT ...` to the end of our query, to return only the number of rows specified.

```
sqlite> SELECT * FROM movies LIMIT 10;
movie_id   title                       year  type   minutes
---------  --------------------------  ----  -----  -------
tt0012349  The Kid                     1921  movie  68
tt0015324  Sherlock Jr.                1924  movie  45
tt0015864  The Gold Rush               1925  movie  95
tt0017136  Metropolis                  1927  movie  153
tt0017925  The General                 1926  movie  78
tt0019254  The Passion of Joan of Arc  1928  movie  110
tt0021749  City Lights                 1931  movie  87
tt0022100  M                           1931  movie  117
tt0025316  It Happened One Night       1934  movie  105
tt0027977  Modern Times                1936  movie  87
```
or if you used Python:
```
> cur.execute("SELECT * FROM movies LIMIT 10;")
> cur.fetchall()
[('tt0012349', 'The Kid', 1921, 'movie', 68),
 ('tt0015324', 'Sherlock Jr.', 1924, 'movie', 45),
 ('tt0015864', 'The Gold Rush', 1925, 'movie', 95),
 ('tt0017136', 'Metropolis', 1927, 'movie', 153),
 ('tt0017925', 'The General', 1926, 'movie', 78),
 ('tt0019254', 'The Passion of Joan of Arc', 1928, 'movie', 110),
 ('tt0021749', 'City Lights', 1931, 'movie', 87),
 ('tt0022100', 'M', 1931, 'movie', 117),
 ('tt0025316', 'It Happened One Night', 1934, 'movie', 105),
 ('tt0027977', 'Modern Times', 1936, 'movie', 87)]
 ```

(The rest of the examples will have the SQL query separate from the result, to allow easy copy-pasting into your own SQLite session or Python program).

#### Selecting specific columns

`SELECT *` returns all columns from a table or query. If we only want some of the columns, we can choose to select/project out only those of interest. We can list them in any order; in the relational model, attributes (the columns of our table) are fundamentally unordered.

```sql
SELECT movie_id, title, year FROM movies LIMIT 10;
```
```
movie_id   title                       year
---------  --------------------------  ----
tt0012349  The Kid                     1921
tt0015324  Sherlock Jr.                1924
tt0015864  The Gold Rush               1925
tt0017136  Metropolis                  1927
tt0017925  The General                 1926
tt0019254  The Passion of Joan of Arc  1928
tt0021749  City Lights                 1931
tt0022100  M                           1931
tt0025316  It Happened One Night       1934
tt0027977  Modern Times                1936
```

We can also rename columns if we want, using the `AS` keyword. This can be helpful when making columns with derived data in, and is essential when joining tables with columns of the same name:
```sql
SELECT year AS release_year FROM movies LIMIT 1;
```
```
release_year
------------
1921
```

#### Sorting results

Perhaps we are more interested in recent movies; if we wanted to see the 10 most _recent_ movies in the database, we would need to sort the results in descending order before limiting them. To do that we can use SQL's `ORDER BY ...` which, as you might expect, goes before the limit:
```sql
SELECT movie_id, title, year FROM movies ORDER BY year DESC LIMIT 10;
```
```
movie_id    title                                    year
----------  ---------------------------------------  ----
tt0439572   The Flash                                2023
tt10366206  John Wick: Chapter 4                     2023
tt12263384  Extraction II                            2023
tt12758060  Tetris                                   2023
tt1517268   Barbie                                   2023
tt15398776  Oppenheimer                              2023
tt16419074  Air                                      2023
tt22297828  Chor Nikal Ke Bhaga                      2023
tt24268454  The Kerala Story                         2023
tt2906216   Dungeons & Dragons: Honor Among Thieves  2023
```
As queries get longer, you can optionally write them over multiple lines for clarity and to increase readability:
```sql
SELECT movie_id, title, year
FROM movies
ORDER BY year DESC
LIMIT 10;
```
You can also order by multiple columns, noting that ascending order (`ASC`) is the default if you don't specify an order after the column name:
```sql
SELECT movie_id, title, year FROM movies ORDER BY year DESC, title LIMIT 10;
```
```
movie_id    title                                          year
----------  ---------------------------------------------  ----
tt16419074  Air                                            2023
tt1517268   Barbie                                         2023
tt22297828  Chor Nikal Ke Bhaga                            2023
tt2906216   Dungeons & Dragons: Honor Among Thieves        2023
tt12263384  Extraction II                                  2023
tt6791350   Guardians of the Galaxy Vol. 3                 2023
tt10366206  John Wick: Chapter 4                           2023
tt9603212   Mission: Impossible - Dead Reckoning Part One  2023
tt15398776  Oppenheimer                                    2023
tt9362722   Spider-Man: Across the Spider-Verse            2023
```

### Filtering using WHERE

Normally we don't want to select every row of the table. The `WHERE` clause allows us to select only rows satisfying the condition provided. This condition is evaluated row-by-row on the selected rows and only rows where the condition is True are included. The condition can be anything from the trivial (`WHERE TRUE`, which would include every row; `WHERE FALSE` which would include nothing at all) to the complex, built up using logical operations like `AND`, `OR` and `NOT`.

```sql
SELECT movie_id, title, year FROM movies WHERE type='movie' AND year >= 2013 LIMIT 5;
```
```
movie_id   title                            year
---------  -------------------------------  ----
tt0359950  The Secret Life of Walter Mitty  2013
tt0369610  Jurassic World                   2015
tt0437086  Alita: Battle Angel              2019
tt0439572  The Flash                        2023
tt0448115  Shazam!                          2019
```

The usual comparison operators work for both text values and numeric values; `=`, `>`, `<`, `>=`, `<=`, `<>` (or often `!=` too), and brackets `()`can be used to group operations. SQL also supports some basic pattern-matching for text values using the `LIKE` keyword; the `%` character is a wildcard, representing a match of any characters of any length; and `_` represents a single-character wildcard.

```sql
SELECT title FROM movies WHERE title LIKE 'Star %';
```
```
title
----------------------------------------------
Star Wars: Episode IV - A New Hope
Star Wars: Episode V - The Empire Strikes Back
Star Wars: Episode VI - Return of the Jedi
Star Wars: Episode I - The Phantom Menace
Star Wars: Episode II - Attack of the Clones
Star Wars: Episode III - Revenge of the Sith
Star Trek
Star Trek Into Darkness
Star Wars: Episode VII - The Force Awakens
Star Wars: Episode VIII - The Last Jedi
Star Trek Beyond
```

It's worth highlighting that `WHERE` filtering happens _after_ the rows have been selected; it doesn't matter for these simple queries, but can become important with complicated JOINs.


### Aggregating data using GROUP BY

Often we want to aggregate data together to give summary values. For example, suppose we wanted to know how many movies were made each year since 2018. We can use the GROUP BY construct to group rows together by shared attribute value, and then use an aggregate function like `count` to return a single result for that shared value. The `AS` keyword lets us name the result of this aggregate function. We'll use `count(*)` to mean "count how many rows in this group"; you can use `count(1)` or indeed any constant non-null value; the star highlights that it doesn't matter what part of the row we count. 
```sql
SELECT year, count(*) AS n_movies FROM movies WHERE year > 2018 GROUP BY year ORDER BY year;
```
```
year  n_movies
----  --------
2018  56
2019  59
2020  26
2021  42
2022  42
2023  16
```

When using `GROUP BY`, any attribute selected in the `SELECT` clause **must** be in the `GROUP BY` clause or inside an aggregate function. The only exception to this is when you are grouping by the primary key of one table in a JOIN, in which case you can select other attributes from that table (since they are unique for that primary key value being grouped by).

In the example above, imagine we tried to do `SELECT year, type, count(*) FROM movies GROUP BY year;`. For the grouped row with `year` equal to 2018, there are 56 different values for `type` in the rows we grouped together; which value should we pick for the returned row? In almost every relational database system except SQLite, attempting to run this query will throw an error. [SQLite is different](https://sqlite.org/quirks.html#aggregate_queries_can_contain_non_aggregate_result_columns_that_are_not_in_the_group_by_clause); it will pick a row arbitrarily, which may well not be what you want and may hide a mistake you have made. Take care here! 

Of course, if we wanted to know how many movies of each type there were per year, we could use the query `SELECT year, type, count(*) FROM movies GROUP BY year, type;` which will do what we want, but will return multiple rows for each year; one for each of the types of movie released that year. This will **not** have the same result as the previous query! It will also not return rows for the "no movies of this type were released this year" case; i.e. there will not be any zero rows. Think about why this is, and what it might be possible to do if you wanted these zeroes too.

`GROUP BY` comes after `WHERE` but before `ORDER BY` and `LIMIT`. A `WHERE` clause filters which rows are selected for inclusion in the grouping; it is used before the grouping starts. This means you cannot use the results of an aggregate to do filtering directly. If we want to select years with at least 50 movies released in them, we **cannot** use `WHERE count(*) >= 50`; this is a syntax error, aggregate functions cannot appear in a WHERE clause. We either have to use a nested subquery:
```sql
SELECT year, total FROM (SELECT year, count(*) AS total FROM movies GROUP BY year) AS subquery WHERE total >= 50;
```
or we can use the `HAVING` keyword, which is exactly equivalent to the nested query form above:
```sql
SELECT year, count(*) AS total FROM movies GROUP BY year HAVING total > 50;
```
`HAVING` filters the rows resulting from the grouping, `WHERE` filters the rows _before_ the grouping.


### Other aggregate functions

We used `count(*)` above, and remarked that we could have used `count(1)` or any non-null value. If instead we use a column name, like `count(movie_id)`, we will count how many non-null rows there are. There are no non-null values in the movies table, but this ability to count only non-null values will be more useful later on.

There is also a `DISTINCT` keyword we can use inside the count function, for example if we want to know how many different types of movie were released each year. Some years will have `tvMovies` and `movies`, say, and so have a count of 2; others only have one type and so have a count of 1.
```sql
SELECT year, count(DISTINCT type) FROM movies GROUP BY year ORDER BY year DESC LIMIT 5;
```
```
year  count(DISTINCT type)
----  --------------------
2023  1
2022  1
2021  2
2020  2
2019  2
```

There are other aggregate functions; `avg` (mean), `sum`, `min` and `max` for numeric values:
```sql
SELECT min(votes), max(votes), avg(votes), sum(votes) FROM ratings;
```
```
min(votes)  max(votes)  avg(votes)        sum(votes)
----------  ----------  ----------------  ----------
5448        2771997     348167.154466085  518420893
```
We used the ratings table here, since it makes sense to think about average number of votes and total number of votes on movies. Notice we didn't include a `GROUP BY` clause but used aggregate functions; since there are no non-aggregate columns, the database groups all the rows into one group and computes these values on that full set of rows.

### Example query 1

At this point we can look at a fuller example that uses most of the constructs above, to highlight the order we build up an SQL query in:
```sql
SELECT year, count(*) AS n_movies
FROM movies
WHERE (year >= 2000 AND year < 2010) AND type='movie'
GROUP BY year
ORDER BY n_movies DESC
LIMIT 3;
```
What does this query do? It is looking at the movies table, including only cinema movies from the 2000's, grouping them by year and counting how many there are per-year. It then orders these rows by the number of movies in each year and limits this to the top three. So the query finds the top three years in the 2000's with the largest number of cinema movies that year.


### Using JOINs

The real power of a relational database lies in the ability to join tables of related data together; almost everything up to this point could have been a glorified spreadsheet.

For our first `JOIN`, we will join together the `movies` and `ratings` tables to get the ratings for each movie with the title of the movie. `ratings` has a foreign key `movie_id` that links to the primary key of the `movies` table; we join on these values being equal. Each row in `ratings` corresponds to exactly one row in `movies` and vice versa, so this is a very simple join.
```sql
SELECT title, rating, votes FROM movies JOIN ratings ON movies.movie_id=ratings.movie_id
ORDER BY rating DESC LIMIT 5;
```
```
title                             rating  votes
--------------------------------  ------  -------
Threat Level Midnight: The Movie  9.6     9882
The Shawshank Redemption          9.3     2771997
The Godfather                     9.2     1929661
12 Angry Men                      9       822267
The Godfather Part II             9       1312387
```

This is an inner join (indeed we could have written `movies INNER JOIN ratings`, the `INNER` keyword is entirely optional). It uses the `JOIN ... ON ...` construct, with the `ON` clause used to decide which pairs of rows from all possible pairings should be included. There are other older ways to write this join; the following are all equivalent for an inner join:
```sql
-- modern, best-practice join:
SELECT * FROM movies JOIN ratings ON movies.movie_id=ratings.movie_id;
-- throwback to the dark ages, the implicit comma-join:
SELECT * FROM movies, ratings WHERE movies.movie_id=ratings.movie_id;
-- cross-join, equivalent to comma-join but clearer:
SELECT * FROM movies CROSS JOIN ratings WHERE movies.movie_id=ratings.movie_id;
```
The `JOIN ... ON ...` syntax is preferable, for clarity and because for outer joins the `WHERE` clause behaves differently. 

#### Natural joins

In this case we could also have used a `NATURAL JOIN`. Natural joins join tables on **all** shared column names; here the only shared column name was `movie_id` and the join would have behaved as we might have expected. Note that foreign keys are irrelevant to a natural join; the join is based solely on the column _names_ and nothing more.

In the real world, `NATURAL JOIN` is considered fragile and dangerous; the columns of a table are not fixed and a query that once worked can easily break if a new column is added that happens to have a shared name. The logic of how the join works is no longer in the query but determined by the current state of the database. Additionally, consider what happens when two tables have a common column name that is not related, such as a `last_updated` time; there are unlikely to be circumstances where we would want to join on equal last updated timestamps for different objects! Never use a `NATURAL JOIN`.

#### Multiple joins

It is possible to join more than one table; we often want to join several tables together in a normalised database. Consider finding the names of the genres for each movie. We will need to use `movies`, `has_genre` and `genres`:
```sql
SELECT movies.movie_id, title, genres.name FROM movies
JOIN has_genre ON movies.movie_id=has_genre.movie_id
JOIN genres ON has_genre.genre_id=genres.genre_id
LIMIT 10;
```
```
movie_id   title          name
---------  -------------  ---------
tt0012349  The Kid        Comedy
tt0012349  The Kid        Drama
tt0012349  The Kid        Family
tt0015324  Sherlock Jr.   Action
tt0015324  Sherlock Jr.   Comedy
tt0015324  Sherlock Jr.   Romance
tt0015864  The Gold Rush  Adventure
tt0015864  The Gold Rush  Comedy
tt0015864  The Gold Rush  Drama
tt0017136  Metropolis     Drama
```
Each movie is associated with more than one genre and for each match via our joins, we get an additional row in the results. Notice that we used `movies.movie_id` in the `SELECT`; there are two `movie_id` columns in our joined set of columns, and we have to choose which one we want (even though we joined on that value and so know they are always equal).


### Example query 2

How might we go about finding the most popular 5 rom-coms? We need to find movies with both the genres `Romance` and `Comedy`, and then order them by their rating votes.
```sql
SELECT title, year, votes
FROM movies
JOIN ratings ON movies.movie_id = ratings.movie_id
JOIN has_genre AS hg1 ON movies.movie_id = hg1.movie_id
JOIN genres AS g1 ON hg1.genre_id = g1.genre_id
JOIN has_genre AS hg2 ON movies.movie_id = hg2.movie_id
JOIN genres AS g2 ON hg2.genre_id = g2.genre_id
WHERE g1.name='Romance' AND g2.name='Comedy'
ORDER BY votes DESC
LIMIT 5;
```
```
title                    year  votes
-----------------------  ----  ------
Amélie                   2001  773610
Silver Linings Playbook  2012  725171
Life Is Beautiful        1997  716419
Crazy, Stupid, Love.     2011  537985
500 Days of Summer       2009  534903
```
Since we need to join the `genres` and `has_genre` tables twice, we need to give each of them an alias using the `AS` construct so that we can be clear which constraints belong to which copy of the table.


### Outer joins

The joins mentioned so far are all inner joins; if a row pairing doesn't match the predicate, it is excluded. Sometimes we might want to keep a row that does not match the table we are joining to. Consider this join:
```sql
SELECT name, count(movie_id) AS n_movies FROM people
JOIN has_position ON people.person_id = has_position.person_id AND position='actor'
GROUP BY people.person_id
ORDER BY n_movies
LIMIT 3;
```
```
name            n_movies
--------------  --------
Lauren Bacall   1
John Belushi    1
Ingrid Bergman  1
```
Are there not people in our `people` table that have never acted? Why are there no rows with `n_movies` equal to zero? What we want here is to keep the `people` row even if it does not match a `has_position` row. A `LEFT JOIN` (or `LEFT OUTER JOIN`, the `OUTER` is entirely optional) will allow us to do this. If the row from the left side of the join does not match a row from the right side table, it is kept anyway and `NULL` values used for all the right table attributes in the resulting row.
```sql
SELECT name, count(movie_id) AS n_movies FROM people
LEFT JOIN has_position ON people.person_id = has_position.person_id AND position='actor'
GROUP BY people.person_id
ORDER BY n_movies
LIMIT 3;
```
```
name             n_movies
---------------  --------
Ingmar Bergman   0
Georges Delerue  0
Jerry Goldsmith  0
```
Voilà; now the query keeps the non-actors. The count doesn't count the `NULL` `movie_id` values that result from not matching, and so we get the zeroes we were expecting.

There is a big "gotcha!" hiding here. Notice how the query checks that the position is an actor not in a `WHERE` clause but in the `ON` clause of the join. If we move the position check into a `WHERE` clause, something odd happens:
```sql
SELECT name, count(movie_id) AS n_movies FROM people
LEFT JOIN has_position ON people.person_id = has_position.person_id
WHERE position='actor'
GROUP BY people.person_id
ORDER BY n_movies
LIMIT 3;
```
```
name            n_movies
--------------  --------
Lauren Bacall   1
John Belushi    1
Ingrid Bergman  1
```
Our zeroes are gone again, despite our `LEFT JOIN`! The issue here is that `WHERE` clause filtering happens _after_ the join has happened. If we look at the results of the join before we do any grouping, the problem becomes clearer:

```sql
SELECT people.person_id, name, movie_id, position FROM people
LEFT JOIN has_position ON people.person_id = has_position.person_id AND position='actor'
GROUP BY people.person_id
LIMIT 3;
```
```
person_id  name            movie_id   position
---------  --------------  ---------  --------
nm0000002  Lauren Bacall   tt0276919  actor
nm0000004  John Belushi    tt0080455  actor
nm0000005  Ingmar Bergman  <null>     <null>
```
Notice the `NULL` value in the `position` column for 'Ingmar Bergman'. When our `WHERE` clause then filters these rows, it will compare `NULL='actor'`, which results in `NULL`, and skip the row because `NULL` is false-y. So we end up removing the left-joined rows again.

By using a `WHERE` clause, we have turned our outer join back into an inner join! Beware using columns from the right-hand table in `WHERE` clauses of left joins; put them in the join `ON` clause instead.

`RIGHT JOIN` also exists, but it is unnecessary; you can always swap the order of the tables in the join around to make a right join a left join or vice versa. What if you want to keep both the left side rows that don't match _and_ the right hand side rows that don't match? A `FULL OUTER JOIN` will do this (but SQLite's support for this is poor if `LIMIT`s are involved).


### Subqueries

The results of any `SELECT` query can be treated like a table and used inside another `SELECT` query. This can be helpful for building up more complex queries. The `HAVING` clause example earlier used a subquery as an alternative:
```sql
SELECT year, total FROM (SELECT year, count(*) AS total FROM movies GROUP BY year) AS subquery WHERE total >= 50;
```
Here we do some extra filtering on the result of the inner query. SQLite does not insist on a unique name for the subquery, but some other DBMSes do require it.

These subqueries can be nested arbitrarily deep, though the readability decreases quite quickly:
```sql
SELECT * FROM
    (SELECT * FROM
         (SELECT * FROM people WHERE deathyear IS NULL) AS living_actors
    WHERE birthyear <= 1950) AS older_living_actors
WHERE name LIKE '%John%';
```

#### IN and NOT IN

We can treat the results of a subquery like a set, and use the `IN` and `NOT IN` operators to check if a value exists in that set in a `WHERE` clause. This can be a powerful alternative to a join, although it is often worth testing both approaches with real data before deciding one way is better than the other.

Consider finding people who have composed for a movie. We could do this in two different ways:
```sql
SELECT DISTINCT people.* 
FROM people JOIN has_position ON people.person_id = has_position.person_id
WHERE position='composer';
-- or
SELECT * FROM people WHERE person_id IN (SELECT person_id FROM has_position WHERE position='composer');
```
Notice that we have to include a `DISTINCT` in the join version, since if a person composed for multiple movies they would appear multiple times in the results. Using the `person_id IN (...)` approach avoids this because we are just selecting rows directly from `people` and each person only appears once.

Consider finding people who have never acted; we can do this with `NOT IN`:
```sql
SELECT * FROM people WHERE person_id NOT IN (SELECT person_id FROM has_position WHERE position='actor');
```
Take care with `NOT IN`, however. If there are `NULL` values in the subquery, it will do very unexpected things!

The matching is not limited to a single column, either; it is possible to say `WHERE (a, b) IN (SELECT c, d FROM table)` and so on.

### VIEWs and Common Table Expressions

Sometimes it is useful to break a query up into parts, either because it is complicated or because bits of it are useful in many other queries. A `VIEW` is a permanent way of saving a `SELECT` query into the database, giving it a name and allowing it to be used as if it was a table in subsequent queries.

We can create a view that contains only people who have composed for movies:
```sql
CREATE VIEW composers AS 
    SELECT * FROM people WHERE person_id IN (SELECT person_id FROM has_position WHERE position='composer');
```
and then use that view by name in subsequent queries:
```sql
SELECT * FROM composers;
```

When creating views, note that they are permanent; this view will stay around in our database, potentially for other users to access it. If we make lots of views, it can quickly get confusing. Views are stored as queries; when we refer to it, the query is executed.

In practice, it is unlikely that you will want to make a view unless it is a really common subquery or a very helpful abstraction. You can remove views you have made by name:
```sql
DROP VIEW composers;
```

#### Common Table Expressions

Rather than using permanent views, it can be helpful to use named subqueries declared up-front. These are called "common table expressions" (CTEs), and are declared using the `WITH` clause. They behave in exactly the same way as a view, but last only for the single query they occur in. They can refer to earlier CTEs in the `WITH` clause, too:
```sql
WITH
    romances AS (SELECT movie_id FROM has_genre JOIN genres ON genres.genre_id = has_genre.genre_id AND name='Romance'),
    comedies AS (SELECT movie_id FROM has_genre JOIN genres ON genres.genre_id = has_genre.genre_id AND name='Comedy'),
    rom_coms AS (SELECT movie_id FROM romances WHERE movie_id IN (SELECT movie_id FROM comedies))
SELECT * FROM movies WHERE movie_id IN (SELECT movie_id FROM rom_coms);
```
This query finds all rom-coms. Unlike making views, the names `romances`, `comedies` and `rom_coms` don't exist outside this query. This might be easier to follow than the approach using joins taken in Example 2!


### Set operations

In the rom-com example above, we were trying to find the intersection of the set of movies that were romances with the set of movies that were comedies. Rather than the clunky `WHERE movie_id IN ...` approach above, we can use the fact that SQL supports set operations to make this more clear.
```sql
WITH
    romances AS (SELECT movie_id FROM has_genre JOIN genres ON genres.genre_id = has_genre.genre_id AND name='Romance'),
    comedies AS (SELECT movie_id FROM has_genre JOIN genres ON genres.genre_id = has_genre.genre_id AND name='Comedy'),
    rom_coms AS (SELECT movie_id FROM romances INTERSECT SELECT movie_id FROM comedies)
SELECT * FROM movies WHERE movie_id IN (SELECT movie_id FROM rom_coms);
```

There is also `EXCEPT` for set difference, `UNION` for set union removing duplicate values, and `UNION ALL` for a union operation preserving duplicate values. Note that in most SQL dialects you can include brackets around the sides of these set operations, but SQLite does not support this.
```sql
-- not allowed by SQLite:
(SELECT 1 AS n) UNION (SELECT 2 AS n);
-- allowed:
SELECT 1 AS n UNION SELECT 2 AS n;
```


### VALUES clauses

When inserting data, we need a row-like object containing some data we're providing from outside the database. We may also want to make a quick table of temporary data. The `VALUES` clause allows us to do this.

```sql
WITH temp(a, b, c) AS (VALUES (1, 2, 3), (4, 5, 6))
SELECT * FROM temp;
```
```
a  b  c
-  -  -
1  2  3
4  5  6
```

It is most commonly seen when doing an `INSERT`:
```sql
INSERT INTO movies VALUES ('fake_id', 'fake_title', 999, 'movie', -1);
-- let's clean up our mess right now:
DELETE FROM movies WHERE movie_id='fake_id';
```

One neat use of the values clause is to generate a truth table, so we can test the equivalence of boolean expressions:
```sql
WITH v(x) AS (VALUES (TRUE), (FALSE), (NULL)),
     truth_table AS (
         -- Get all pairings using a cross join!
        SELECT v.x AS a, v2.x AS b FROM v CROSS JOIN v AS v2
     )
SELECT a, b,
       (a AND b) IS NULL AS form_one,
       (a IS NULL) OR (b IS NULL) AS form_two
FROM truth_table;
```
```
a       b       form_one  form_two
------  ------  --------  --------
1       1       0         0
1       0       0         0
1       <null>  1         1
0       1       0         0
0       0       0         0
0       <null>  0         1
<null>  1       1         1
<null>  0       0         1
<null>  <null>  1         1
```

## Final notes

At this point it is probably worth clarifying that you do not have to write SQL keywords in capital letters. The practice is a throwback to the days when SQL was written in basic text processors with no syntax highlighting, in order to make the keywords stand out and improve readability. Capitalised keywords can help the reader distinguish between keywords and column names when reserved words have been used as column names (like our `year` for movies, which is also an SQL keyword). Using lowercase works just as well, but note that column names _are_ case-sensitive in many DBMSes (unlike keywords). Technically mixed-case also works but, unless you're trying to be an internet meme, maybe don't write `SeLeCt * FrOm movies LiMiT 1;`.

For historical reasons, SQLite [does not enforce foreign key constraints by default](https://www.sqlite.org/foreignkeys.html#fk_enable). If you decide to experiment with the `INSERT`, `UPDATE` and `DELETE` commands, you will likely want to turn on foreign key constraints using `PRAGMA foreign_keys = ON;` _each time you connect to the database_.
