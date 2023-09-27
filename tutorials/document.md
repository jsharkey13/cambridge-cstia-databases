# Document Database

## Introduction

This tutorial is designed to get you started with our document database and ready to complete the document database Tick. Again we'll be using a database of movies, cast and crew information from the [Internet Movie Database (IMDb)](https://developer.imdb.com/non-commercial-datasets/). The data included is the same as the relational database, so it should be possible to compare query outputs for consistency.

We'll be using [TinyDB](https://tinydb.readthedocs.io/en/latest/index.html) for our document database. TinyDB is a toy document database, chosen because it is simple and easy to set up. Importantly, its query interface is fairly close to those of libraries for accessing real document databases like [MongoDB](https://www.mongodb.com/) or [ElasticSearch](https://www.elastic.co/). Under the hood, TinyDB uses JSON to store the data, and so it supports storing arbitrary JSON documents which it maps to Python dictionaries.

## Installation

Once you have Python installed, you can install the latest version of TinyDB by running:
```bash
pip install tinydb
```
TinyDB has no dependencies and is written in pure Python, so it will run anywhere that can run Python.

Once installed, in a Python program import `tinydb` and connect to the database JSON file:
```python
import tinydb
from tinydb import Query

tdb = tinydb.TinyDB('/path/to/movies.tinydb.json')
```

Note the second import line. We are going to need this useful function soon, and we have imported it directly because we will use it extensively:


### Getting the data

Download the JSON file [movies.tinydb.json](!!!!!) and save it somewhere sensible. This file contains our entire database. If you decide to play with the data modification and insert commands later on, perhaps download a fresh copy of the database before attempting the Tick.


## TinyDB by example

TinyDB has the concept of "tables"; in the same way relational databases group similar objects into tables, we do the same in TinyDB. In other document databases this form of grouping may be called a "collection" (in MongoDB) or an "index" (in ElasticSearch, and not to be confused with a relational or search index; same name, very different concept!). Unlike relational tables, in TinyDB these tables are only for logically grouping documents together; documents in the same table do not have to have similar structure, and there is no schema.

Every document in a TinyDB table has an ID, called `doc_id`. It is possible to use custom classes to extend TinyDB to use string IDs, but by default it uses integers. This means we haven't used the `movie_id` and `person_id` fields as document IDs; the `doc_id` values are arbitrary.

The documents returned from TinyDB are Python dictionaries. We can access their properties using the standard dictionary access (`doc["attribute_name"]`) and if desired we can turn them into plain JSON using the `json` library with `json.dumps(doc)`.

Our TinyDB has two tables, `movies` and `people`.

```python
tdb_movies = tdb.table("movies")
tdb_people = tdb.table("people")
```

Every query below this point assumes the 5 lines of Python above, importing and declaring the table variables, have already been run in your script or interpreter. 

### Document schemas?

The best way to see what these tables contain is to look at a document from each of them. After all, there is no fixed schema, just the documents!

```python
> tdb_people.get(doc_id=1)
{'person_id': 'nm0000002',
 'name': 'Lauren Bacall',
 'birthyear': 1924,
 'deathyear': 2014,
 'acted_in': [{'movie_id': 'tt0276919',
               'title': 'Dogville',
               'year': 2003,
               'roles': ['Ma Ginger']}]}
```
This is a Python dictionary (not quite JSON, note the single-quotes) representing a person JSON document. It has the same attributes as the relational `people` table; `person_id`, `name`, `birthyear`, `deathyear`. However, it has additional attributes too; we have de-normalised the data and collected everything about this person into a single object. We now have an additional `acted_in` attribute, that contains a list of the movies the person has acted in and the roles they played.

What about someone who never acted, but only directed?
```python
> tdb_people.get(doc_id=3)
{'person_id': 'nm0000005',
 'name': 'Ingmar Bergman',
 'birthyear': 1918,
 'deathyear': 2007,
 'directed': [{'movie_id': 'tt0050976', 'title': 'The Seventh Seal', 'year': 1957},
              {'movie_id': 'tt0050986', 'title': 'Wild Strawberries', 'year': 1957},
              {'movie_id': 'tt0060827', 'title': 'Persona', 'year': 1966}]}
```
This time we don't have an `acted_in` attribute but a `directed` attribute instead. Our people documents do not have to have a fixed schema, we can exclude attributes that aren't relevant. The possible attributes for the positions people had are `acted_in`, `directed`, `produced`, `wrote`, and `composed_for`. Inside each is a list of movie objects that contain the `movie_id`, `title`, `year` and may contain `job` information. Someone who acted and directed would have both `acted_in` and `directed` attributes, for example.

Document databases have no required schema, so there's nothing to stop documents representing people using `birthyear` sometimes, `birthYear` other times, and `birth_year` elsewhere. In Python, attributes are normally named in `snake_case`, but in JSON they are often in `camelCase` due to JavaScript. In this database attributes have consistent names, chosen to be the same as the relational attributes, but are absent when unknown or irrelevant. There is also nothing to stop us from adding a movie document to the people table, either deliberately or accidentally; the table names are for logical grouping, but they don't enforce anything!

In general, with a schemaless document database system, it is up to the application using it to impose some sort of structure on the documents it stores. Since it turned out that most of the time at least a loose structure to documents is desired, most modern document databases allow some form of schema validation to prevent mistakes.

These notes aside, what about documents in the `movies` table?
```python
> tdb_movies.get(doc_id=1)
{'movie_id': 'tt0012349',
 'title': 'The Kid',
 'year': 1921,
 'type': 'movie',
 'minutes': 68,
 'genres': ['Comedy', 'Drama', 'Family'],
 'rating': 8.3,
 'rating_votes': 130363,
 'actors': [{'person_id': 'nm0088471', 'name': 'B.F. Blinn', 'roles': ['His Assistant']},
            {'person_id': 'nm0000122', 'name': 'Charles Chaplin', 'roles': ['A Tramp']},
            {'person_id': 'nm0701012', 'name': 'Edna Purviance', 'roles': ['The Woman']},
            {'person_id': 'nm0001067', 'name': 'Jackie Coogan', 'roles': ['The Child']},
            {'person_id': 'nm0588033', 'name': 'Carl Miller', 'roles': ['The Man']},
            {'person_id': 'nm0042317', 'name': 'Albert Austin', 'roles': ['Car Thief', 'Man in Shelter']},
            {'person_id': 'nm0047800', 'name': 'Beulah Bains', 'roles': ['Bride']},
            {'person_id': 'nm0048798', 'name': 'Nellie Bly Baker', 'roles': ['Slum Nurse']},
            {'person_id': 'nm0074788', 'name': 'Henry Bergman', 'roles': ['Professor Guido', 'Night Shelter Keeper']},
            {'person_id': 'nm0080930', 'name': 'Edward Biby', 'roles': ['Orphan Asylum Driver']}]}
```
We can see that it again has attributes much like the relational `movies` table did: `movie_id`, `title`, `type`, `minutes`. Additionally, the genres associated with the movie are stored as a list by name in the `genres` key. The actors and the roles they play are also stored as objects in the `actors` key. If the movie had director information, they would be listed under `directors`; likewise with `producers`, `writers` and `composers`.

We don't need to use `null` values in our JSON documents, we can just exclude the key entirely. There is a lot of duplication and redundancy in this document format; the same movie titles and people's names occur in many different documents. For a dataset like historic movie data, this is unlikely to be an issue; in the film industry people rarely change their professional name and movies are unlikely to be renamed. In other use-cases, this redundancy might present more challenges.


### Basic searches

Since the `doc_id` values are meaningless to us, we need to query for documents of interest. There are two equivalent ways of building TinyDB queries, and you can [read the TinyDB documentation](https://tinydb.readthedocs.io/en/latest/usage.html#queries) for full details if you wish. We will use the `Query()` approach in this tutorial.

#### Getting a single document

Say we remembered from the relational tutorial that the 'Barbie' movie ID was `tt1517268`. If we want just one document from a table, we can use the `.get(...)` method on the table object, with a query:
```python
m = Query()
tdb_movies.get(m.movie_id == 'tt1517268')
```
This returns the 'Barbie' movie document:
```python
{'movie_id': 'tt1517268',
 'title': 'Barbie',
 'year': 2023,
 'type': 'movie',
 'minutes': 114,
 'genres': ['Adventure', 'Comedy', 'Fantasy'],
 'rating': 7.5,
 'rating_votes': 98056,
 'actors': [{'person_id': 'nm3053338', 'name': 'Margot Robbie', 'roles': ['Barbie']},
            {'person_id': 'nm0331516', 'name': 'Ryan Gosling', 'roles': ['Ken']},
            {'person_id': 'nm4793987', 'name': 'Issa Rae', 'roles': ['Barbie']},
            {'person_id': 'nm0571952', 'name': 'Kate McKinnon', 'roles': ['Barbie']}],
 'directors': [{'person_id': 'nm1950086', 'name': 'Greta Gerwig'}],
 'producers': [{'person_id': 'nm3943537', 'name': 'Tom Ackerley', 'job': 'producer'},
               {'person_id': 'nm0107509', 'name': 'Robbie Brenner', 'job': 'producer'},
               {'person_id': 'nm0382268', 'name': 'David Heyman', 'job': 'producer'}],
 'writers': [{'person_id': 'nm0000876', 'name': 'Noah Baumbach', 'job': 'written by'}],
 'composers': [{'person_id': 'nm1053148', 'name': 'Mark Ronson'}]}
```

We can also use the "fragment" approach, to find a document which contains the fragment provided:
```python
 tdb_movies.get(Query().fragment({'movie_id': 'tt1517268'}))
```
This returns the same document as before. Notice that we don't have to make the `Query` object separately if we're not going to refer to it more than once; we can just use it inline.

Once we have a document, we can access attributes of it as with any Python dictionary:
```python
> barbie = tdb_movies.get(Query().movie_id == 'tt1517268')
> # Use square brackets, may raise KeyError if document missing this attribute:
> barbie["title"]
'Barbie'
> # Or equivalently use the "get" method, which will return None if missing:
> barbie.get("year")
2023
> barbie["actors"][0]
{'person_id': 'nm3053338', 'name': 'Margot Robbie', 'roles': ['Barbie']}
```

#### Finding multiple documents

Often we will want to find multiple documents that match some criteria. We can use the table's `.search(...)` method to achieve this. To get a list of all movies created after 2021:
```python
tdb_movies.search(Query().year >= 2021)
```
```python
[{'movie_id': 'tt0439572',
  'title': 'The Flash',
  'year': 2023,
  # ...
  },
 # and 99 more movies . . .
 ]
``` 

TinyDB doesn't offer any limiting or sorting; at the end of the day it runs in-memory on the machine running the query, so any limiting or sorting can be done by the user directly. If we want to sort by most recent by `year`, we can do it directly with Python's built-in `sorted` function. The `sorted` method can take a `key` function that decides what attribute(s) of an object to sort on:
```python
sorted(tdb_movies.search(Query().year >= 2021), key=lambda m: m.get("year"), reverse=True)
```
We could also limit it to only 10 of these movies using Python's list slicing:
```python
sorted(tdb_movies.search(Query().year >= 2021), key=lambda m: m.get("year"), reverse=True)[:10]
```

Full-featured document databases do usually support sorting and limiting, because they use a client-server model where transferring unnecessary data comes with a cost.

#### Counting results

Rather than a list of all the matching documents, we may just want to know how many of them there are. Instead of `.search(...)`, we can use `.count(...)` with exactly the same query arguments.

How many movies are there created after 2021?
```python
tdb_movies.count(Query().year >= 2021)
```
This returns `100`.

Subsequent examples will use `count` rather than `search`, to reduce the amount of output printed. You can always simply swap to `search` to see the full list. You could also use the relational database as a way of checking that the result numbers are correct.


#### Other types of conditions

We've seen how to match an attribute exactly, and we've seen how to do numeric range searches. TinyDB supports other search conditions too.

If we want to find movies whose titles start with "Star ", we can use `.matches(...)` with a regular expression. This is more powerful than the SQL `LIKE` clause supported by SQLite (though many other relational DBMSes do support regular expressions). The regex pattern `.*` matches any character any number of times, so is equivalent to the `%` of SQL `LIKE`.
```python
tdb_movies.count(Query().title.matches("Star .*"))
```

What about finding a movie with a specific genre? Recall that the `genres` attribute for movies is a list of genre names. We can do an "any" match on a list attribute to require any element to match the one(s) provided. To find all "War" films:
```python
tdb_movies.count(Query().genres.any(['War']))
```

We can also do an "all" match on a list attribute, to require that all the items provided are contained in the list. To find all rom-coms, we can simply do:
```python
tdb_movies.count(Query().genres.all(['Romance', 'Comedy']))
```
Compare this to the SQL it took to compute the same list; the de-normalisation of the data makes some things much easier.

If the objects in the list are more complex, we can provide another `Query()` that operates on the sub-documents in that list. To find all movies directed by Steven Spielberg, we can do:
```python
tdb_movies.count(Query().directors.any(Query().name == 'Steven Spielberg'))

# Or we could name the Query objects for readability:
movie = Query()
director = Query()
tdb_movies.count(movie.directors.any(director.name == 'Steven Spielberg'))
```

If instead we want to match if a non-list attribute is one of a list of known values, we can use `one_of(...)`:
```python
tdb_movies.count(Query().movie_id.one_of(["tt4975722", "tt3169706"]))
```

It might also be enough to know that an attribute exists on an object. Since only composers have an `composed_for` attribute, we can quickly find everyone who has composed for a movie using the `exists` predicate. We don't care what values are in the attribute, just that the key exists. (Of course, this makes assumptions about the non-empty nature of the `composed_for` list, but we can check this).
```python
tdb_people.count(Query().composed_for.exists())

# Check this gives same result as requiring at least one movie in the list:
tdb_people.count(Query().composed_for.any(Query().noop()))
```
Here we used `Query().noop()`, which always matches every document!

Sometimes the check you want to do on the attribute contents is even more complicated. You can use `.test(...)` to provide an arbitrary function which returns True if the document should be matched. Here we find movies with at least 5 actors:
```python
tdb_movies.count(Query().actors.test(lambda actor_list: len(actor_list) >= 5))
```

#### Combining conditions

Often, the conditions we want to search on are more complicated than a single value. TinyDB allows the combining of query conditions using the Python bitwise "and", "or" and "not" operators (`&`, `|` and `~` respectively). This comes with a caveat; these operators bind more tightly than equals and other comparisons, so each query condition **must** be wrapped in brackets first.

As in the relational tutorial, to find movies of type 'movie' and made since 2013:
```python
tdb_movies.count((Query().year >= 2013) & (Query().type == 'movie'))
```

To find people who have never acted:
```python
tdb_people.count(~(Query().acted_in.exists()))
```

You can combine conditions arbitrarily using these operators to perform really complex filtering.

### Some limitations

Whilst the lack of sorting and ability to restrict the number of results are limitations only of TinyDB, document databases do have other limitations compared to the relational examples we saw before.

#### JOINs

It might seem obvious, but document databases don't support anything like relational join operations when searching for documents. Instead, when creating the database and adding documents, we have to de-normalise the data ourselves. The movies documents contain the genres, cast and crew data that would be obtained by joining in the relational database.


Consider trying to find movies with non-unique titles. Document databases don't allow you to directly compare documents to each other. That action is fundamentally the same thing as joining a table to itself.

The following query looks at first glance like it might do what we want:
```python
# These names will prove to be misleading:
m1 = Query()
m2 = Query()
# This demonstrates a fundamental misunderstanding in how querying works!
tdb_movies.count((m1.title == m2.title) & (m1.movie_id < m2.movie_id))
```
The way the `Query()` objects are named here is just confusing. When searching for documents, the query is tested against each document individually. It describes what form the documents we want to match must have; we cannot **compare** to other documents, both `m1` and `m2` refer to the same document when testing the condition. (And the `Query()` object behaves strangely when you compare it to itself, rather than a constant, as done here!).

How might we do it ourselves in TinyDB? We could write a `.test(...)` function that itself queries to find out how many movies there are with that title, and only includes the row if there is more than one:
```python
# WARNING! This is _exceptionally_ slow and may hang your interpreter for some time!
tdb_movies.count(Query().title.test(lambda t: tdb_movies.count(Query().title == t) > 1))
```
Whilst this does work correctly, it is incredibly inefficient. It will help to have a model about how TinyDB works under the hood. All the query methods work by simply iterating over every document in the table and evaluating the condition on each document in turn. So our test function loops over every movie and checks the title, then counts those that match. We then run this function once per movie! In terms of complexity, we have made something at least O(n^2) in the number of movies.

It is possible to iterate through all the movies ourselves. The table object can be used as an iterator and we can collect the documents by title and then filter them ourselves:
```python
titles = dict()
for movie_doc in tdb_movies:
    title = movie_doc["title"]
    if title not in titles:
        titles[title] = []
    titles[title].append(movie_doc)

matched_movie_docs = []
for title, movie_list in titles.items():
    if len(movie_list) > 1:
        matched_movie_docs.extend(movie_list)
```
There are many other valid ways of doing this, but at least this approach is better than O(n^2) in the number of movies.

#### Aggregation

TinyDB has no support for aggregation of any kind. If we wanted to know how many movies were made each year, we would have to do so ourselves. MongoDB, a large commercial document database with a much richer feature set, does support aggregation queries. They work in a similar way to SQL, except you have to say _how_ to turn the documents into groups; in SQL you simply declare the output you want. In MongoDB you first need to project out the bits of the documents you are interested in, then define how to group, and then produce counts/aggregate statistics or output.

As above, in TinyDB it is possible to iterate through all the documents yourself and compute the aggregate statistics you want.

