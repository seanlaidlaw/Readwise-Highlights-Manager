#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import requests
import json
import sqlite3
import math
from datetime import datetime

if not "API_TOKEN_READWISE" in os.environ:
    raise SystemExit("Error: Environment variable API_TOKEN_READWISE is not set")

highlights_database = "readwise_highlights.sqlite3"

# get current date-time in UTC to save to database on successful sync with Readwise
last_run_log_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# if database doesn't exist then create it and create tables
if not os.path.isfile(highlights_database):
    connection = sqlite3.connect(highlights_database)
    cursor = connection.cursor()
    sql_create_articleids_table = """CREATE TABLE ArticleIds (
								id INTEGER,
								updated  TEXT
							);"""
    sql_create_bookids_table = """CREATE TABLE BookIds (
								id INTEGER,
								updated  TEXT
							);"""
    sql_create_highlights_table = """CREATE TABLE Highlights (
								id INTEGER,
								category  TEXT,
								updated  TEXT,
								note   TEXT,
								book_id  INTEGER
						);"""
    sql_create_lastrun_log = """CREATE TABLE Log (
							id INTEGER PRIMARY KEY AUTOINCREMENT,
							updated  TEXT
						);"""

    cursor.execute(sql_create_highlights_table)
    cursor.execute(sql_create_bookids_table)
    cursor.execute(sql_create_articleids_table)
    cursor.execute(sql_create_lastrun_log)


readwise_categories = ["books", "articles", "tweets", "supplementals", "podcasts"]


def getBooksFromCategory(category, updated_filter=None):
    pages_of_results = []
    page_number = 1

    # get total number of books in category by writing a small request to see the total count
    response = requests.get(
        url="https://readwise.io/api/v2/books/",
        headers={
            "Authorization": os.environ["API_TOKEN_READWISE"],
        },
        params={
            "category": category,
            "page_size": 1,
            "page": 1,
            "updated__gt": updated_filter,
        },
    )

    data = response.json()
    print(data)
    # round up the count / 1000 to get the number of pages needed to request if we have 1000 results per page
    total_pages = int(math.ceil(data["count"] / 1000))

    # for each of the pages we need to request, make a request and append its results to a list
    while page_number <= total_pages:
        querystring = {
            "category": category,
            "page_size": 1000,
            "updated__gt": updated_filter,
            "page": page_number,
        }

        response = requests.get(
            url="https://readwise.io/api/v2/books/",
            headers={
                "Authorization": os.environ["API_TOKEN_READWISE"],
            },
            params=querystring,
        )
        data = response.json()
        pages_of_results.append(data["results"])

        page_number = page_number + 1

    return pages_of_results


#    for result in results:
#        cursor.execute(
#            "Insert into BookIds values (?, ?)", (result["id"], result["updated"])
#        )
#        connection.commit()
#


# Get ids of all books in Readwise
# get ids of all articles in Readwise
def getArticlesList(updated_filter=None):
    querystring = {
        "category": "articles",
        "page_size": 1000,
        "updated__gt": None,
        "page": 1,
    }

    response = requests.get(
        url="https://readwise.io/api/v2/books/",
        headers={
            "Authorization": os.environ["API_TOKEN_READWISE"],
        },
        params=querystring,
    )

    data = response.json()
    results = data["results"]
    return results

    #    for result in results:
    #        cursor.execute(
    #            "Insert into ArticleIds values (?, ?)",
    #            (result["id"], result["updated"]),
    #        )
    #        connection.commit()

    # get list of book_ids from database and get highlights for each book
    cursor.execute("SELECT DISTINCT id FROM BookIds")
    data = cursor.fetchall()
    for book_id in data:
        book_id = book_id[0]

        # Make API request to get highlight data for each book
        querystring = {
            "book_id": book_id,
            "page_size": 1000,
            "updated__gt": None,
            "page": 1,
        }

        time.sleep(4)
        response = requests.get(
            url="https://readwise.io/api/v2/highlights/",
            headers={
                "Authorization": os.environ["API_TOKEN_READWISE"],
            },
            params=querystring,
        )

        data = response.json()
        print(data)

        results = data["results"]

        for result in results:
            cursor.execute(
                "Insert into Highlights values (?, ?, ?, ?, ?)",
                (
                    result["id"],
                    result["text"],
                    result["updated"],
                    result["note"],
                    result["book_id"],
                ),
            )
            connection.commit()

    # get list of book_ids from database and get highlights for each book
    cursor.execute("SELECT DISTINCT id FROM ArticleIds")
    data = cursor.fetchall()
    for book_id in data:
        book_id = book_id[0]

        # Make API request to get highlight data for each book
        querystring = {
            "book_id": book_id,
            "page_size": 1000,
            "updated__gt": None,
            "page": 1,
        }

        # readwise rate limit is 20 requests/min
        # 60s / 20 = 3 so to play it safe wait 4s between requests
        time.sleep(4)
        response = requests.get(
            url="https://readwise.io/api/v2/highlights/",
            headers={
                "Authorization": os.environ["API_TOKEN_READWISE"],
            },
            params=querystring,
        )

        data = response.json()
        print(data)

        results = data["results"]

        for result in results:
            cursor.execute(
                "Insert into Highlights values (?, ?, ?, ?, ?)",
                (
                    result["id"],
                    result["text"],
                    result["updated"],
                    result["note"],
                    result["book_id"],
                ),
            )
            connection.commit()

    # Update Log table in sqlite with the current date/time string so next time
    # its run it can get only the new highlights and not re-request everything
    cursor.execute("Insert into Log values (NULL, ?)", ([last_run_log_str]))
    connection.commit()
