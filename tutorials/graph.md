# Graph Database

## Introduction

The graph database is an optional part of the course, and so the focus here will be on installation and basic querying only. Again we'll be using a database of movies, cast and crew information from the [Internet Movie Database (IMDb)](https://developer.imdb.com/non-commercial-datasets/). The data included is the same as the relational and document databases, so it should be possible to compare query outputs for consistency.

We'll be using [Neo4j](https://neo4j.com/) for the graph database; it is the most popular graph NoSQL database, and uses a language called Cypher instead of SQL.


## Installation

Neo4j, as the name suggests, requires Java. We'll need Java 11 or higher. If you don't already have a JDK installed, then the [Eclipse Temurin OpenJDK](https://adoptium.net/en-GB/temurin/releases/) is likely the best place to start.

Once Java is installed, install the [Community Server version of Neo4j](https://neo4j.com/download-center/#community). Use version 4.4, the LTS version, since the database to be loaded was created using this version. Once downloaded and extracted, you should find you have a `bin` directory inside the Neo4j folder which contains the scripts you'll need to manage the Neo4j server.

The very first thing to do is to start the server and change the default login credentials; you can't use Neo4j until this is completed.

Start the neo4j server:
```bash
/path/to/bin/neo4j console
```
(On Windows you have a choice between Command Prompt, `neo4j.bat`; or Powershell, `neo4j.ps1`, as the script to execute. Since the Command Prompt script just loads Powershell, it may be best to use a Powershell terminal so that Neo4j exits more cleanly when you quit).

If you have trouble with Java, you can manually set the `JAVA_HOME` environment variable for your shell session to point to the directory where the correct version of Java is installed. This can be used to override which version of Java it is that Neo4j attempts to load.

Once the server has started, it should state `Remote interface available at http://localhost:7474/`. Go to [this Neo4j Browser URL](http://localhost:7474/), and you should see the Neo4j Browser interface and a `:server connect` dialog. The default host and port are correct; use the default login `neo4j` with the password `neo4j` to connect to the database. You will immediately need to change the password; you can change this to any value, except the existing `neo4j`.

Having set the password, you can stop the server using Ctrl-C in the terminal.

### Getting the data

Download the Neo4j database dump file [movies.neo4j.dump](!!!!!) and save it somewhere sensible.

With the server stopped, we can load data using an admin script. We need to use `--force` to overwrite the existing (empty) default `neo4j` database. If you have used the community edition of Neo4j before or have other data in Neo4j, don't proceed here!
```bash
/path/to/bin/neo4j-admin load --force --from=/path/to/movies.neo4j.dump
```

Start the server again using `neo4j console`, and then check Neo4j Browser; you should now see `Movie` and `Person` listed as "Node labels" and the various relationships listed under "Relationship types".


## Cypher by example

Neo4j has the concept of "nodes" and "relationships", that form the vertices and edges of our graph. Nodes have "labels" that describe their type, and can have attributes called "keys". Relationships are very similar, but must connect two nodes together.

The [full Cypher manual](https://neo4j.com/docs/cypher-manual/4.4/introduction/) can be found on the Neo4j website, alongside an official tutorial and introduction videos; we will just cover some simple examples.

### Schema

Like other NoSQL databases, Neo4j does not enforce a schema. There are two types of nodes in our database, labelled `Movie` and `Person`, which intuitively correspond to our movie and people objects. They have the same attribute names as the relational database, and the attributes are absent when they are `NULL`.

The relationships between people and movies have the same names as the document database person attributes but in capitals: `ACTED_IN`, `DIRECTED`, `PRODUCED`, `WROTE` and `COMPOSED_FOR`. They optionally have the `job` attribute as before.

### Basic matching

The Cypher language is similar to SQL; it is a declarative language where you state what you want to match and what you want returned from that match. The matching is declared in an almost ASCII-art syntax. For example, to match a single movie node:
```cypher
MATCH (m:Movie)
RETURN m
LIMIT 1;
```

Here we have stated what we want to match: nodes are matched by `()`, optionally with their label such as `:Movie`; they also optionally have a name so that we can refer to them later in the query, here `m`. Once matched, in the Neo4j Browser you can view the node in a graph, not so useful here with only one node; view the node in a table; or as plain-text output like the command-line interface.

Of course, we probably don't want an arbitrary movie. There are multiple equivalent ways to specify attributes of the movie to match:
```cypher
MATCH (m:Movie {title: 'Barbie'})
RETURN m
LIMIT 1;

// Or, equivalently:

MATCH (m:Movie)
WHERE m.title='Barbie'
RETURN m
LIMIT 1;
```

Nodes with the label `Person` can be matched in exactly the same way:
```cypher
MATCH (p:Person)
RETURN p
LIMIT 1;
```

We can match relationships by themselves, too. These are matched using square brackets, again optionally with a label:
```cypher
MATCH ()-[r:ACTED_IN]-()
RETURN r
LIMIT 1;
```
Since we did not return the nodes at either end of the relationship, this isn't particularly useful. However, it does show the pattern matching nature of Cypher. We matched a relationship between two unnamed nodes, but we could add further constraints.

Say we wanted to know every movie directed by Steven Spielberg:
```cypher
MATCH (:Person {name: 'Steven Spielberg'})-[:DIRECTED]-(m:Movie)
RETURN m;
```
This only returns the movies, but we can match the whole subgraph if we are just exploring the data. The previous form is likely more useful to a program, but this form shows the graph nature of Neo4j well:
```cypher
MATCH path=(:Person {name: 'Steven Spielberg'})-[:DIRECTED]-(:Movie)
RETURN path;
```

### Aggregation

Patterns to match can be more complicated than single pairs of nodes, and it is not necessary to return nodes and relationships themselves either.

The following query counts how many movies a pair of actors have acted in together (and highlights that the dataset we are using is incomplete; the results are not quite as you would expect if _all_ actors were listed for all movies):
```cypher
MATCH (p1:Person) - [:ACTED_IN] -> (m:Movie) <- [:ACTED_IN] - (p2:Person)
WHERE p1.person_id < p2.person_id
RETURN p1.name as name1, p2.name as name2, count(*) as total
ORDER BY total desc, name1, name2
LIMIT 10;
```

### Longer path matching

It is possible to match longer paths along relationships without typing them all out. The `*` operator allows us to quantify how many relationship hops to make. If we wanted to find a Bacon-number style link between Jennifer Lawrence and Daniel Radcliffe, for example, we can squash the `[:ACTED_IN]` hops together.

A `*` by itself is unbounded; this will likely match far too many paths. Instead, we can list a specific number (remembering that it must be even!), using `[:ACTED_IN*2]` and then `*4`, `*6` and `*8` until we finally find a path that works:
```cypher
MATCH path=(m:Person {name : 'Jennifer Lawrence'})
            -[:ACTED_IN*8]-
           (n:Person {name : 'Daniel Radcliffe'})
RETURN path;
```

Or we could have bounded the number of hops to search directly, to between 2 and 10:
```cypher
// This will match a lot of nodes, be careful!
MATCH path=(m:Person {name : 'Jennifer Lawrence'})
            -[:ACTED_IN*2..10]-
           (n:Person {name : 'Daniel Radcliffe'})
RETURN path;
```
Since this returns _all_ paths that match, of any allowed length, there will be a large number of nodes and relationships returned. Every `(:Person)-[:ACTED_IN]-(:Movie)-[:ACTED_IN]-(:Person)` grouping on every single possible path between these two actors is a valid path returned by the query.

How do we know if we have found the shortest path? Neo4j has a `shortestpath(...)` function we can use, and we can return the length of the path and not the path itself. This time we can use an unbounded number of hops, since longer paths can be efficiently discarded during the search:
```cypher
MATCH path=shortestpath(
    (m:Person {name : 'Jennifer Lawrence'})
     -[:ACTED_IN*]-
    (n:Person {name : 'Daniel Radcliffe'}))
RETURN length(path);
```

### Bacon number, efficiently

Finally, we can use the graph database to compute Bacon numbers efficiently. This is where the power of using a graph database with efficient graph storage and built-in graph traversal operations comes into its own.

Rather than just `shortestpath(...)` we can use the related `allshortestpaths(...)` to return shortest possible paths for many different `Person` nodes. Since Bacon number is traditionally the co-actor distance, this is half the number of `[:ACTED_IN]` hops we find: 
```cypher
MATCH path=allshortestpaths(
          (m:Person {name : 'Kevin Bacon'} ) -[:ACTED_IN*]- (n:Person))
WHERE n.person_id <> m.person_id
RETURN length(path)/2 AS bacon_number,
       count(distinct n.person_id) AS total
ORDER BY bacon_number;
```

This is vastly clearer and more concise than something equivalent in SQL!


## Final notes

As with SQL, it is worth pointing out that Cypher is not case-sensitive and the keywords can be written in lowercase without issue. The relationship labels (e.g. `ACTED_IN`) are case-sensitive though. Capital letters are used for keywords here only to clearly distinguish them.
