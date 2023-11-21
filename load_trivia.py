import os
import random
import time

import tinydb
from tinydb import Query
from playwright.sync_api import sync_playwright

IMDB_BASE = "https://www.imdb.com"
IMDB_TRIVIA_BASE = IMDB_BASE + "/title/{}/trivia/"
OUTPUT_DIRECTORY = "output"
SQLITE_FILENAME = os.path.join(OUTPUT_DIRECTORY, "movies.sqlite")
TINYDB_BASE_FILENAME = os.path.join(OUTPUT_DIRECTORY, "movies.tinydb.json")
TINYDB_FILENAME = os.path.join(OUTPUT_DIRECTORY, "movies-trivia.tinydb.json")


playwright = sync_playwright().start()
chrome = playwright.chromium.launch(headless=False)
page = chrome.new_page()

page.goto(IMDB_BASE)


###

# Open original database:
tdb_orig = tinydb.TinyDB(TINYDB_BASE_FILENAME)
tdb_movies_orig = tdb_orig.table("movies")
total = len(tdb_movies_orig)

# Open augmented database:
tdb = tinydb.TinyDB(TINYDB_FILENAME)
tdb_movies = tdb.table("movies")

for i, movie in enumerate(tdb_movies_orig):
    movie_id = movie.get("movie_id")
    if tdb_movies.count(Query().movie_id == movie_id) != 1:
        print("{}/{} - {}".format(i+1, total, movie_id))
        # We haven't yet loaded this movie!

        imdb_url = IMDB_TRIVIA_BASE.format(movie_id)
        page.goto(imdb_url)
        time.sleep(0.5 + random.random()*2)

        # Hide the rating modal if necessary:
        no_prompt = page.locator("xpath=//button[contains(.//span, 'prompt me to rate')]")
        if no_prompt.is_visible():
            no_prompt.click()
        # Load more trivia items?
        # see_more = page.locator("xpath=//button[contains(.//span, 'more')]")
        # if see_more:
        #     see_more.scroll_into_view_if_needed()
        #     see_more.click()
        # time.sleep(0.5 + random.random()*2)

        trivia_items = page.locator("xpath=//div[@class='ipc-html-content-inner-div']").all_inner_texts()
        # At most 5 entries, ensuring all are non-empty text:
        trivia_items = [t for t in trivia_items if type(t) is str and t][:5]
        movie["trivia_entries"] = trivia_items

        tdb_movies.insert(movie)

        # Scrape at a human-like speed:
        time.sleep(10 + random.random() * 10)
    else:
        print("{}/{} - SKIPPING {}".format(i+1, total, movie_id))

chrome.close()

tdb_people_orig = tdb_orig.table("people")
tdb_people = tdb.table("people")

for person in tdb_people_orig:
    tdb_people.insert(person)

tdb.close()
tdb_orig.close()
