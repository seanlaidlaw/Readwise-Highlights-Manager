#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import requests
import json
import sqlite3

if not "API_TOKEN_READWISE" in os.environ:
    raise SystemExit("Error: Environment variable API_TOKEN_READWISE is not set")

highlights_database = "readwise_highlights.sqlite3"

# if database doesn't exist then create table else connect to database
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

    cursor.execute(sql_create_highlights_table)
    cursor.execute(sql_create_bookids_table)
    cursor.execute(sql_create_articleids_table)
else:
    connection = sqlite3.connect(highlights_database)
    cursor = connection.cursor()


# Get list of all books in Readwise
querystring = {"category": "books", "page_size": 1000, "page": 1}

response = requests.get(
    url="https://readwise.io/api/v2/books/",
    headers={"Authorization": os.environ["API_TOKEN_READWISE"]},
    params=querystring,
)

data = response.json()
if data["count"] <= 1000:
    results = data["results"]
    for result in results:
        cursor.execute(
            "Insert into BookIds values (?, ?)", (result["id"], result["updated"])
        )
        connection.commit()


# get ids of all articles in Readwise
querystring = {"category": "articles", "page_size": 1000, "page": 1}

response = requests.get(
    url="https://readwise.io/api/v2/books/",
    headers={"Authorization": os.environ["API_TOKEN_READWISE"]},
    params=querystring,
)

data = response.json()
if data["count"] <= 1000:
    results = data["results"]
    for result in results:
        cursor.execute(
            "Insert into ArticleIds values (?, ?)", (result["id"], result["updated"])
        )
        connection.commit()


# get list of book_ids from database and get highlights for each book
cursor.execute("SELECT DISTINCT id FROM BookIds")
data = cursor.fetchall()
for book_id in data:
    book_id = book_id[0]

    # Make API request to get highlight data for each book
    querystring = {"book_id": book_id, "page_size": 1000, "page": 1}

    time.sleep(30)
    response = requests.get(
        url="https://readwise.io/api/v2/highlights/",
        headers={"Authorization": os.environ["API_TOKEN_READWISE"]},
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
    querystring = {"book_id": book_id, "page_size": 1000, "page": 1}

    time.sleep(30)
    response = requests.get(
        url="https://readwise.io/api/v2/highlights/",
        headers={"Authorization": os.environ["API_TOKEN_READWISE"]},
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
