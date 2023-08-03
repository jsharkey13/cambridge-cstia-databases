# Movies Databases

This repo contains code to generate several different databases containing the same data, for the [Cambridge NST IA Databases course](https://www.cl.cam.ac.uk/teaching/2324/Databases/). 

The data is a subset of [the IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/). The list of movies is filtered to only include popular movies, or those with useful properties such as duplicate names. The cast and crew data is then filtered down to only include those people relevant to this movies subset. The aim is to output ~1500 movies, to keep the database accessible even on basic hardware.

Three databases are created; [SQLite 3](https://www.sqlite.org/index.html), [TinyDB](https://tinydb.readthedocs.io/en/latest/index.html) and [Neo4j](https://neo4j.com/), to demonstrate relational, document and graph databases respectively.

## Installation

Install Python 3.7 or later, clone this repo and then install the required dependencies:

```bash
pip install requests tinydb neo4j
```

You will also need to install Neo4j, and Java 11 in order to run Neo4j. This code was designed with the Community Server edition in mind, version 4.4 LTS. Examples may not work in the 5.x version of Neo4j.

## Usage 

The main file is [`make_databases.py`](make_databases.py). This script will download the relevant IMDb files, and create the databases.

Run:

```bash
python make_databases.py
```

The SQLite and TinyDB outputs will be created if they do not exist, or emptied and recreated if they do. The script expects a Neo4j database to be already running on `localhost` with the default port; credentials should be configured in `neo4j/neo4j_credentials.json` in the form `{"username": "neo4j", "password": "neo4j"}`. All existing nodes and relations in the database `neo4j` will be deleted and the movies data loaded; this is the default and only available database in the community server version.

The script will create `output/movies.sqlite` and `output/movies.tinydb.json`, as well as loading the data into the `neo4j` database in the running Neo4j server.

### Derived outputs

This script creates the _databases_ themselves. For SQLite and TinyDB, these are conveniently the single-file artefacts needed for someone to create their own version. Some additional artefacts are necessary:

#### SQL file

It is useful to have a plain SQL file of CREATE and INSERT statements for use with other relational databases. Once you have run the script, connect to the SQLite database with `sqlite3 output/movies.sqlite` and then dump the output to a file:
```bash
.output output/movies.sql
.dump
```
This file is not plain SQL; it will likely contain an SQLite PRAGMA command at the start. In a text editor, remove this line to produce a vendor-agnostic output.

#### Neo4j database export

To export the Neo4j database to file, stop the database server and then use the admin command to dump it to a file:

```bash
/path/to/neo4j-admin dump --database=neo4j --to=/path/to/output/movies.neo4j.dump
```

## Tutorials

There are tutorials for using the two main databases for the course: [relational database tutorial](tutorials/relational.md); [document database tutorial](tutorials/document.md).
