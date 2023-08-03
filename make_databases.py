import contextlib
import csv
import gzip
import json
import os
import sqlite3

import requests
import tinydb

import neo4j

IMDB_BASE_URL = 'https://datasets.imdbws.com'
IMDB_DIRECTORY = 'imdb'
IMDB_FILES = {
    "people": "name.basics.tsv.gz",
    "film_alternate_titles": "title.akas.tsv.gz",
    "film_titles": "title.basics.tsv.gz",
    "film_crews": "title.crew.tsv.gz",
    "tv_episodes": "title.episode.tsv.gz",
    "films_people": "title.principals.tsv.gz",
    "film_ratings": "title.ratings.tsv.gz"
}

INCLUDE_TYPES = ["movie", "tvMovie", "video"]
INCLUDE_ROLES = ["actor", "actress", "director", "producer", "writer", "self", "composer"]

OUTPUT_DIRECTORY = "output"
SQLITE_FILENAME = os.path.join(OUTPUT_DIRECTORY, "movies.sqlite")
TINYDB_FILENAME = os.path.join(OUTPUT_DIRECTORY, "movies.tinydb.json")

IMDB_TOP_250 = ["tt0111161", "tt0068646", "tt0468569", "tt0071562", "tt0050083",
                "tt0108052", "tt0167260", "tt0110912", "tt0120737", "tt0060196",
                "tt0109830", "tt0137523", "tt0167261", "tt1375666", "tt9362722",
                "tt0080684", "tt0133093", "tt0099685", "tt0073486", "tt0114369",
                "tt0038650", "tt15398776", "tt0047478", "tt0102926", "tt0816692",
                "tt0120815", "tt0317248", "tt0118799", "tt0120689", "tt0076759",
                "tt0103064", "tt0088763", "tt0245429", "tt0253474", "tt0054215",
                "tt6751668", "tt0110413", "tt0172495", "tt0110357", "tt0120586",
                "tt0407887", "tt2582802", "tt0482571", "tt0114814", "tt0034583",
                "tt0095327", "tt0056058", "tt1675434", "tt0027977", "tt0095765",
                "tt0064116", "tt0047396", "tt0078748", "tt0021749", "tt0078788",
                "tt0209144", "tt1853728", "tt0082971", "tt0910970", "tt0405094",
                "tt0043014", "tt0050825", "tt4154756", "tt0081505", "tt0032553",
                "tt0051201", "tt4633694", "tt0090605", "tt0169547", "tt1345836",
                "tt0057012", "tt0361748", "tt0364569", "tt2380307", "tt0086879",
                "tt0114709", "tt0112573", "tt0082096", "tt7286456", "tt4154796",
                "tt0119698", "tt0119217", "tt0087843", "tt5311514", "tt1187043",
                "tt0045152", "tt0057565", "tt0180093", "tt8267604", "tt0435761",
                "tt0091251", "tt0086190", "tt0338013", "tt0062622", "tt2106476",
                "tt0105236", "tt0056172", "tt0044741", "tt0033467", "tt0022100",
                "tt0053125", "tt0053604", "tt0052357", "tt0211915", "tt0036775",
                "tt0066921", "tt0093058", "tt0086250", "tt8503618", "tt1255953",
                "tt0113277", "tt1049413", "tt0056592", "tt0070735", "tt1832382",
                "tt0097576", "tt0017136", "tt0095016", "tt0119488", "tt0208092",
                "tt0040522", "tt0986264", "tt0075314", "tt8579674", "tt5074352",
                "tt0363163", "tt1745960", "tt0059578", "tt0372784", "tt0012349",
                "tt0053291", "tt10272386", "tt0993846", "tt0042192", "tt6966692",
                "tt0055031", "tt0120382", "tt0089881", "tt0112641", "tt0469494",
                "tt0457430", "tt1130884", "tt0105695", "tt0167404", "tt0107290",
                "tt0268978", "tt0040897", "tt0055630", "tt0477348", "tt0071853",
                "tt0266697", "tt0057115", "tt0084787", "tt0042876", "tt0266543",
                "tt0080678", "tt10872600", "tt0071315", "tt0434409", "tt0081398",
                "tt0031381", "tt0046912", "tt0347149", "tt0120735", "tt1305806",
                "tt2096673", "tt1392214", "tt5027774", "tt0050212", "tt0117951",
                "tt0116282", "tt1291584", "tt1205489", "tt0264464", "tt0096283",
                "tt0405159", "tt0118849", "tt4729430", "tt1201607", "tt0083658",
                "tt0015864", "tt2024544", "tt0112471", "tt2278388", "tt0052618",
                "tt2267998", "tt0047296", "tt0072684", "tt0017925", "tt0107207",
                "tt2119532", "tt0077416", "tt0050986", "tt0041959", "tt0353969",
                "tt0046268", "tt0015324", "tt3011894", "tt0031679", "tt1392190",
                "tt0097165", "tt0198781", "tt0978762", "tt0892769", "tt0073195",
                "tt0050976", "tt3170832", "tt0046438", "tt0118715", "tt1950186",
                "tt0382932", "tt0019254", "tt0395169", "tt0075148", "tt0091763",
                "tt3315342", "tt1895587", "tt0088247", "tt15097216", "tt0381681",
                "tt1979320", "tt0074958", "tt0092005", "tt0036868", "tt0758758",
                "tt0032138", "tt0113247", "tt0317705", "tt0070047", "tt0325980",
                "tt0035446", "tt0107048", "tt0476735", "tt0032551", "tt0058946",
                "tt1028532", "tt4016934", "tt0245712", "tt0048473", "tt0032976",
                "tt0061512", "tt0059742", "tt0025316", "tt0053198", "tt0060827",
                "tt0129167", "tt1454029", "tt0079470", "tt0103639", "tt0099348"]

# Force-include some otherwise-excluded movies, and ensure the top 250 are kept:
KEEP_MOVIE_IDS = ["tt5697572", "tt4877122", "tt0389790", "tt2724064",
                  "tt0113243", "tt1289401", "tt6105098", "tt0074751",
                  "tt0360717", "tt0064505", "tt0115433", "tt0055254",
                  "tt6139732"] + IMDB_TOP_250


##########
# Useful classes and fucntions:
##########

@contextlib.contextmanager
def get_tsvgz_reader(filename):
    gzfile = gzip.open(filename, mode='rt', encoding='utf-8')
    tsv_reader = csv.DictReader(gzfile, delimiter='\t')
    yield tsv_reader
    gzfile.close()


def positions_to_docs(roles_list, role_name, *, extra_keys=None):
    global people
    docs = []
    for role in roles_list:
        if role.category != role_name:
            continue
        doc = {'person_id': role.person_id, 'name': people[role.person_id].name}
        # To make each sub-document, we may want to dynamically add extra keys:
        for key in (extra_keys or []):
            val = getattr(role, key, None)
            if val is not None:
                # A consistent schema? Where we're going we don't _need_ schemas...
                doc[key] = val

        docs.append(doc)
    return docs


def movies_to_docs(roles_list, role_name, *, extra_keys=None):
    global movies
    docs = []
    for role in roles_list:
        if role.category != role_name:
            continue

        movie = movies[role.movie_id]
        doc = {'movie_id': role.movie_id, 'title': movie.title, 'year': movie.year}
        # To make each sub-document, we may want to dynamically add extra keys:
        for key in (extra_keys or []):
            val = getattr(role, key, None)
            if val is not None:
                # Who needs schemas anyway?
                doc[key] = val

        docs.append(doc)
    return docs


def is_empty_val(value):
    if value is None:
        return True
    if isinstance(value, list) and len(value) == 0:
        return True
    # Otherwise the value is meaningful:
    return False


class DataObject:

    def to_sql_params(self):
        # We need a dictionary of all the property names, and Python has one built-in:
        return self.__dict__

    def to_neo4j_dict(self):
        return {k: (v if not isinstance(v, DataObject) else v.__dict__) for k, v in self.__dict__.items()}


class Movie(DataObject):
    def __init__(self, data):
        self.movie_id = data["tconst"]
        self.type = data["titleType"]
        self.title = data["primaryTitle"]
        self.age_restricted = data["isAdult"] != '0'
        self.year = int(data["startYear"]) if data["startYear"].isdecimal() else None
        self.end_year = int(data["endYear"]) if data["endYear"].isdecimal() else None
        self.duration = int(data["runtimeMinutes"]) if data["runtimeMinutes"].isdecimal() else None
        self.genres = data["genres"].split(",") if data["genres"] and "\\N" != data["genres"] else []

    def __str__(self):
        return "<Movie:\n\tID: {}\n\tTitle: {}\n\tType: {}\n\tYear: {}\n\tDuration: {} mins\n\tGenres: {}\n>".format(
                self.movie_id, self.title, self.type, self.year, self.duration, self.genres)


class MovieRating(DataObject):
    def __init__(self, data):
        self.movie_id = data["tconst"]
        self.rating = float(data["averageRating"]) if data["averageRating"].replace(".", "", 1).isdecimal() else None
        self.votes = int(data["numVotes"])

    def __str__(self):
        return "<MovieRating: ID={}, Rating={}, Votes={}>".format(self.movie_id, self.rating, self.votes)


class MovieRole(DataObject):
    def __init__(self, data):
        self.movie_id = data["tconst"]
        self.person_id = data["nconst"]
        self.category = data["category"]
        self.job = data["job"] if "\\N" != data["job"] else None
        self.roles = json.loads(data["characters"]) if "\\N" != data["characters"] else []
        self.position = int(data["ordering"]) if data["ordering"].isdecimal() else None

    def __str__(self):
        return "<MovieRole:\n\tMovieID: {}\n\tPersonID: {}\n\tCategory: {}\n\tJob: {}\n\tRoles: {}\n\tPosition: {}\n>".format(
                self.movie_id, self.person_id, self.category, self.job, self.roles, self.position)


class Person(DataObject):
    def __init__(self, data):
        self.person_id = data["nconst"]
        self.name = data["primaryName"]
        self.birth_year = int(data["birthYear"]) if data["birthYear"].isdecimal() else None
        self.death_year = int(data["deathYear"]) if data["deathYear"].isdecimal() else None

    def __str__(self):
        return "<Person:\n\tID: {}\n\tName: {}\n\tBorn: {}\n\tDied: {}\n>".format(
                self.person_id, self.name, self.birth_year, self.death_year)


##########
# Start processing:
##########

print("[GET AND LOAD DATA]")

# Download the raw datafiles if necessary:
print("[DOWNLOAD FILES]")

os.makedirs(IMDB_DIRECTORY, exist_ok=True)

for filename in IMDB_FILES.values():
    filepath = os.path.join(IMDB_DIRECTORY, filename)
    if os.path.exists(filepath):
        print("Skipping existing: {}".format(filename))
        continue
    print("Downloading: {}".format(filename))
    response = requests.get("{}/{}".format(IMDB_BASE_URL, filename))
    response.raise_for_status()
    with open(filepath, mode="wb") as outfile:
        outfile.write(response.content)


# Load movie fragments:
print("[LOAD MOVIE RATINGS]")
movie_ratings = dict()
with get_tsvgz_reader(os.path.join(IMDB_DIRECTORY, IMDB_FILES["film_ratings"])) as ratings_reader:
        for i, rating_data in enumerate(ratings_reader):
            rating = MovieRating(rating_data)
            movie_ratings[rating.movie_id] = rating

print("ratings:", len(movie_ratings))


# Load movies and filter them:
print("[LOAD MOVIES]")
movies = dict()
with get_tsvgz_reader(os.path.join(IMDB_DIRECTORY, IMDB_FILES["film_titles"])) as titles_reader:
    for i, title_data in enumerate(titles_reader):
        movie = Movie(title_data)
        rating = movie_ratings.get(movie.movie_id)

        # Start filtering to reduce the number of movies and remove inappropriate ones:
        if movie.movie_id not in KEEP_MOVIE_IDS:
            if movie.age_restricted:
                continue
            if movie.type not in INCLUDE_TYPES:
                continue
            if not movie.year:
                continue
            if not rating:
                continue
            if movie.type in ["movie", "video"] and rating.votes < 50000:
                continue
            if movie.type == "tvMovie" and rating.votes < 5000:
                continue
            if len(movie.genres) == 0:
                continue
            # We need to reduce the number of movies included a lot, so lets
            # remove the lower rated films, prioritised by how recent they are:
            if movie.year < 1990:
                if rating.rating < 9 and rating.votes < 2E5:
                    continue
            elif movie.year < 2013:
                if rating.rating < 8 and rating.votes < 2E5:
                    continue
            else:
                if rating.rating < 7 and rating.votes < 5E5:
                    continue

        # If not skipped, add to the movies dict:
        movie.rating = rating
        movies[movie.movie_id] = movie

print("movies:", len(movies))


# Clean up the unwanted ratings:
movie_ratings = None  # Allow some garbage collection!


# Load the movie personnel for these movies:
print("[LOAD MOVIE PERSONNEL]")
movie_roles_people = dict()
movie_roles_movies = dict()
with get_tsvgz_reader(os.path.join(IMDB_DIRECTORY, IMDB_FILES["films_people"])) as films_people_reader:
    for film_person_data in films_people_reader:
        role = MovieRole(film_person_data)
        if role.movie_id not in movies:
            continue

        if role.category not in INCLUDE_ROLES:
            continue

        if role.category == "actress":
            # 'Neutralising genders' is to movies as 'reticulating splines' is to ...
            role.category = "actor"
        if role.category == "self":
            # People appearing as themselves might as well be "actors".
            role.category = "actor"

        # We're going to need roles by person and roles by movie later:
        if role.person_id not in movie_roles_people:
            movie_roles_people[role.person_id] = []
        movie_roles_people[role.person_id].append(role)

        if role.movie_id not in movie_roles_movies:
            movie_roles_movies[role.movie_id] = []
        movie_roles_movies[role.movie_id].append(role)

print("roles (by person):", sum([len(roles) for roles in movie_roles_people.values()]))
print("roles (by movie):", sum([len(roles) for roles in movie_roles_movies.values()]))

# Load the relevant people:
print("[LOAD PEOPLE]")
people = dict()
with get_tsvgz_reader(os.path.join(IMDB_DIRECTORY, IMDB_FILES["people"])) as people_reader:
    for person_data in people_reader:
        person = Person(person_data)

        if person.person_id not in movie_roles_people:
            continue

        people[person.person_id] = person

print("people:", len(people))


# Get a set of all known genres, add some extras, and give them numeric IDs:
all_genres = {genre for m in movies.values() for genre in m.genres}
all_genres.update(["Gothic", "Epic", "Experimental"])
genre_ids = {genre: i for i, genre in enumerate(sorted(all_genres), start=1)}

##########
# Start making databases.
##########
print("\n[MAKE DATABASES]")

os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)


# SQLite database:
print("[SQLITE DATABASE]")

# Open and/or create database file:
con = sqlite3.connect(SQLITE_FILENAME)
cur = con.cursor()

# For backwards compatibility, SQLite doesn't enforce foreign keys by default:
con.execute("PRAGMA foreign_keys = ON;")

# Clear up existing database:
print("Cleaning database")
# Drop relationship tables:
cur.execute("DROP TABLE IF EXISTS ratings;")
cur.execute("DROP TABLE IF EXISTS has_genre;")
cur.execute("DROP TABLE IF EXISTS has_position;")
cur.execute("DROP TABLE IF EXISTS plays_role;")
# Drop entity tables:
cur.execute("DROP TABLE IF EXISTS movies;")
cur.execute("DROP TABLE IF EXISTS people;")
cur.execute("DROP TABLE IF EXISTS genres;")

# Movies data:
print("Create: movies")
cur.execute("CREATE TABLE movies(movie_id TEXT PRIMARY KEY, title TEXT, year INT, type TEXT, minutes INT);")
cur.executemany("INSERT INTO movies VALUES (:movie_id, :title, :year, :type, :duration);", [m.to_sql_params() for m in movies.values()])

# People data:
print("Create: people")
cur.execute("CREATE TABLE people(person_id TEXT PRIMARY KEY, name TEXT, birthyear INT, deathyear INT);")
cur.executemany("INSERT INTO people VALUES (:person_id, :name, :birth_year, :death_year);", [p.to_sql_params() for p in people.values()])

# Ratings data:
print("Create: ratings")
cur.execute("CREATE TABLE ratings(movie_id TEXT PRIMARY KEY REFERENCES movies(movie_id), rating NUMERIC, votes INT);")
cur.executemany("INSERT INTO ratings VALUES (:movie_id, :rating, :votes);", [m.rating.to_sql_params() for m in movies.values()])

# Genres data:
print("Create: genres")
genres_params = [{'genre_id': genre_id, 'name': genre_name} for genre_name, genre_id in genre_ids.items()]
cur.execute("CREATE TABLE genres(genre_id INT PRIMARY KEY, name TEXT);")
cur.executemany("INSERT INTO genres VALUES (:genre_id, :name);", genres_params)

# Movie genres data:
print("Create: has_genre")
has_genres_params = [{'movie_id': m.movie_id, 'genre_id': genre_ids[genre]} for m in movies.values() for genre in m.genres]
cur.execute("CREATE TABLE has_genre(movie_id TEXT REFERENCES movies(movie_id), genre_id INT REFERENCES genres(genre_id), PRIMARY KEY (movie_id, genre_id));")
cur.executemany("INSERT INTO has_genre VALUES (:movie_id, :genre_id);", has_genres_params)

# Movie positions data:
print("Create: has_position")
cur.execute("CREATE TABLE has_position(person_id TEXT REFERENCES people(person_id), movie_id TEXT REFERENCES movies(movie_id), position TEXT, job TEXT, PRIMARY KEY (movie_id, person_id, position));")
cur.executemany("INSERT INTO has_position VALUES (:person_id, :movie_id, :category, :job);", [r.to_sql_params() for roles in movie_roles_people.values() for r in roles])

# Movie acting roles data:
print("Create: plays_role")
movie_roles_params = [{'person_id': role.person_id, 'movie_id': role.movie_id, 'role': role_name} for roles in movie_roles_people.values() for role in roles for role_name in role.roles]
cur.execute("CREATE TABLE plays_role(person_id TEXT REFERENCES people(person_id), movie_id TEXT REFERENCES movies(movie_id), role TEXT, PRIMARY KEY (movie_id, person_id, role));")
cur.executemany("INSERT INTO plays_role VALUES (:person_id, :movie_id, :role);", movie_roles_params)

# Commit changes and close database:
con.commit()
con.close()


# TinyDB database:
print("[TinyDB Database]")

# Open and/or create database file:
tdb = tinydb.TinyDB(TINYDB_FILENAME)

# Clear up existing database:
tdb.drop_tables()

# Movies data:
print("Create: movies")
# Use a generator for efficiency.
movie_documents = ({k: v for k, v in {
            'movie_id': m.movie_id,
            'title': m.title,
            'year': m.year,
            'type': m.type,
            'minutes': m.duration,
            'genres': m.genres,
            'rating': m.rating.rating,
            'rating_votes': m.rating.votes,
            # Now things get complicated! Denormalisation galore!
            'actors': positions_to_docs(movie_roles_movies[m.movie_id], 'actor', extra_keys=['roles']),
            'directors': positions_to_docs(movie_roles_movies[m.movie_id], 'director', extra_keys=['job']),
            'producers': positions_to_docs(movie_roles_movies[m.movie_id], 'producer', extra_keys=['job']),
            'writers': positions_to_docs(movie_roles_movies[m.movie_id], 'writer', extra_keys=['job']),
            'composers': positions_to_docs(movie_roles_movies[m.movie_id], 'composer', extra_keys=['job']),
        # Filter this dict to only include keys if they have meaningful values.
        # Schemas, who needs them?
        }.items() if not is_empty_val(v)
    } for m in movies.values())
tdb_movies = tdb.table("movies")
tdb_movies.insert_multiple(movie_documents)

# People data
print("Create: people")

# Again use a generator for efficiency.
people_documents = ({k: v for k, v in {
            'person_id': p.person_id,
            'name': p.name,
            'birthyear': p.birth_year,
            'deathyear': p.death_year,
            'acted_in': movies_to_docs(movie_roles_people[p.person_id], 'actor', extra_keys=['roles']),
            'directed': movies_to_docs(movie_roles_people[p.person_id], 'director', extra_keys=['job']),
            'produced': movies_to_docs(movie_roles_people[p.person_id], 'producer', extra_keys=['job']),
            'wrote': movies_to_docs(movie_roles_people[p.person_id], 'writer', extra_keys=['job']),
            'composed_for': movies_to_docs(movie_roles_people[p.person_id], 'composer', extra_keys=['job']),
        # Filter this dict to only include keys if they have meaningful values.
        # Schemas, who needs them?
        }.items() if not is_empty_val(v)
    } for p in people.values())
tdb_people = tdb.table("people")
tdb_people.insert_multiple(people_documents)

# Since we have denormalised the data, genres, positions and roles are all in
# movies and people; we don't need other tables.
# The only downside is that TinyDB doesn't support string keys natively, so all
# of the doc_ids are monotonic integers. We _could_ subclass Table to fix this,
# but that increases the complexity for the students using it for minimal gains.

tdb.close()


# Neo4j Database
print("[NEO4J DATABASE]")

with open("neo4j/neo4j_credentials.json") as neo4j_creds_file:
    neo4j_creds = json.load(neo4j_creds_file)

n4j_driver = neo4j.GraphDatabase.driver("neo4j://localhost", auth=(neo4j_creds["username"], neo4j_creds["password"]))

# Purge all existing data:
n4j_driver.execute_query("MATCH (n) DETACH DELETE n")
# Add some indices:
n4j_driver.execute_query("CREATE CONSTRAINT movie_id_unique FOR (m:Movie) REQUIRE m.movie_id IS UNIQUE")
n4j_driver.execute_query("CREATE CONSTRAINT person_id_unique FOR (p:Person) REQUIRE p.person_id IS UNIQUE")
n4j_driver.execute_query("CREATE INDEX movie_titles FOR (m:Movie) ON (m.title)")
n4j_driver.execute_query("CREATE INDEX person_names FOR (p:Person) ON (p.name)")

# Movies data:
print("Create: movies")
n4j_driver.execute_query("""
     UNWIND $movies AS movie
     MERGE (m:Movie {
         movie_id: movie.movie_id,
         title: movie.title,
         type: movie.type,
         year: movie.year,
         minutes: movie.duration,
         genres: movie.genres,
         rating: movie.rating.rating,
         rating_votes: movie.rating.votes
     })
 """, movies=[movie.to_neo4j_dict() for movie in movies.values()], database_="neo4j")

# People data:
print("Create: people")
n4j_driver.execute_query("""
     UNWIND $people AS person
     CREATE (p:Person {
         person_id: person.person_id,
         name: person.name,
         birthyear: person.birth_year,
         deathyear: person.death_year
     })
 """, people=[person.to_neo4j_dict() for person in people.values()], database_="neo4j")

# Acted in data:
print("Create: ACTED_IN")
n4j_driver.execute_query("""
    UNWIND $actors AS actor
    MATCH (p:Person {person_id: actor.person_id})
    MATCH (m:Movie {movie_id: actor.movie_id})
    MERGE (p)-[:ACTED_IN {roles: actor.roles}]->(m)
""", actors=[r.to_neo4j_dict() for movie_roles in movie_roles_movies.values() for r in movie_roles if r.category == "actor"], database_="neo4j")

# Director data:
print("Create: DIRECTED")
n4j_driver.execute_query("""
    UNWIND $directors AS director
    MATCH (p:Person {person_id: director.person_id})
    MATCH (m:Movie {movie_id: director.movie_id})
    CREATE (p)-[:DIRECTED {job: director.job}]->(m)
""", directors=[r.to_neo4j_dict() for movie_roles in movie_roles_movies.values() for r in movie_roles if r.category == "director"], database_="neo4j")

# Producer data:
print("Create: PRODUCED")
n4j_driver.execute_query("""
    UNWIND $producers AS producer
    MATCH (p:Person {person_id: producer.person_id})
    MATCH (m:Movie {movie_id: producer.movie_id})
    CREATE (p)-[:PRODUCED {job: producer.job}]->(m)
""", producers=[r.to_neo4j_dict() for movie_roles in movie_roles_movies.values() for r in movie_roles if r.category == "producer"], database_="neo4j")

# Writers data:
print("Create: WROTE")
n4j_driver.execute_query("""
    UNWIND $writers AS writer
    MATCH (p:Person {person_id: writer.person_id})
    MATCH (m:Movie {movie_id: writer.movie_id})
    CREATE (p)-[:WROTE {job: writer.job}]->(m)
""", writers=[r.to_neo4j_dict() for movie_roles in movie_roles_movies.values() for r in movie_roles if r.category == "writer"], database_="neo4j")

# Composer data:
print("Create: COMPOSED_FOR")
n4j_driver.execute_query("""
    UNWIND $composers AS composer
    MATCH (p:Person {person_id: composer.person_id})
    MATCH (m:Movie {movie_id: composer.movie_id})
    CREATE (p)-[:COMPOSED_FOR {job: composer.job}]->(m)
""", composers=[r.to_neo4j_dict() for movie_roles in movie_roles_movies.values() for r in movie_roles if r.category == "composer"], database_="neo4j")


##########
# Done!
##########

print("[DONE]")
