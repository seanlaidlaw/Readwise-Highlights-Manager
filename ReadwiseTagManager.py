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


def createDatabase(highlights_database):
    """
    Creates the highlights database.

    Args:
        highlights_database: write your description
    """
    connection = sqlite3.connect(highlights_database)
    cursor = connection.cursor()
    sql_create_itemids_table = """CREATE TABLE ItemIds (
                                        id INTEGER PRIMARY KEY,
                                        category TEXT,
                                        title TEXT,
                                        author TEXT,
                                        cover_url TEXT,
                                        readwise_page TEXT,
                                        source_url TEXT,
                                        tags TEXT
                                );"""
    sql_create_highlights_table = """CREATE TABLE Highlights (
                                        id INTEGER PRIMARY KEY,
                                        text TEXT,
                                        note   TEXT,
                                        location INTEGER,
                                        location_type TEXT,
                                        updated  TEXT,
                                        highlighted_at TEXT,
                                        url TEXT,
                                        color TEXT,
                                        book_id  INTEGER,
                                        tags TEXT
                        );"""
    sql_create_lastrun_log = """CREATE TABLE Log (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                last_updated  TEXT
                        );"""

    cursor.execute(sql_create_itemids_table)
    cursor.execute(sql_create_highlights_table)
    cursor.execute(sql_create_lastrun_log)
    return


def getTags(tag_list):
    """
    Convert a list of tags into a space separated string of tag ids

    Args:
        tag_list: write your description
    """
    # get and format tags for a given highlight or article
    # sqlite doesn't support arrays so we are going to input it as
    # space separated string of tag ids
    tag_ids = []
    for tag in tag_list:
        tag_ids.append(str(tag["id"]))
    tag_ids = sorted(set(tag_ids))

    # stringify the tag list
    tag_ids = " ".join(tag_ids)
    return tag_ids


def getTotalPagesOutput(data):
    """
    Returns the total number of pages needed to display the Highlight results.

    Args:
        data: write your description
    """
    # this parses the data json response for the count value and divides it by the max
    # page_size we request (1000) to work out how many pages are needed. as the API requires we
    # give the requested page of Highlight results as a integer we round it up using the
    # math.ceil function
    total_pages = 0
    if "count" in data:
        # round up the count / 1000 to get the number of pages needed to request if we have 1000 results per page
        total_pages = int(math.ceil(float(data["count"]) / float(1000)))
    return total_pages


def getItemsInCategory(category, updated_filter):
    """
    Get all items in a category.

    Args:
        category: write your description
        updated_filter: write your description
    """
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
    total_pages = getTotalPagesOutput(data)

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


def getHighlightsInItem(item_id, updated_filter):
    """
    Returns a list of highlights in the given item.

    Args:
        item_id: write your description
        updated_filter: write your description
    """
    pages_of_results = []
    page_number = 1

    # get total number of books in category by writing a small request to see the total count
    response = requests.get(
        url="https://readwise.io/api/v2/highlights/",
        headers={
            "Authorization": os.environ["API_TOKEN_READWISE"],
        },
        params={
            "book_id": item_id,
            "page_size": 1,
            "page": 1,
            "updated__gt": updated_filter,
        },
    )

    data = response.json()
    total_pages = getTotalPagesOutput(data)

    # for each of the pages we need to request, make a request and append its results to a list
    while page_number <= total_pages:
        querystring = {
            "book_id": item_id,
            "page_size": 1000,
            "updated__gt": updated_filter,
            "page": page_number,
        }

        response = requests.get(
            url="https://readwise.io/api/v2/highlights/",
            headers={
                "Authorization": os.environ["API_TOKEN_READWISE"],
            },
            params=querystring,
        )

        data = response.json()
        pages_of_results.append(data["results"])

        page_number = page_number + 1
        time.sleep(10)

    return pages_of_results


def getDatabaseItemIds(database_cursor):
    """
    Returns a list of database item ids.

    Args:
        database_cursor: write your description
    """
    database_cursor.execute("SELECT DISTINCT id FROM ItemIds")
    data = database_cursor.fetchall()
    item_id_list = []
    for item_id in data:
        item_id = item_id[0]  # we only request one column but it returns list
        item_id_list.append(item_id)

    return item_id_list


def getItemIdsWithTag(database_cursor, tag_id):
    """
    Returns list of item ids with the given tag.

    Args:
        database_cursor: write your description
        tag_id: write your description
    """
    database_cursor.execute(
        "SELECT * FROM ItemIds WHERE tags LIKE '%{}%';".format(tag_id)
    )
    data = database_cursor.fetchall()
    item_id_list = []
    for item_id in data:
        item_id = item_id[0]  # we only request one column but it returns list
        item_id_list.append(item_id)

    return item_id_list


def getLastRunDate(database_cursor):
    """
    Get the last date that a database run was started.

    Args:
        database_cursor: write your description
    """
    database_cursor.execute("SELECT last_updated FROM log ORDER by id DESC LIMIT 1")
    data = database_cursor.fetchall()

    # if not run before then set last_date to start of UNIX time to get all readwise highlights
    if len(data) < 1:
        last_date = "1970-01-01T00:00:00Z"
    else:
        for last_dates in data:
            last_date = last_dates[0]  # we only request one column but it returns list

    return last_date


#### FUNCTION BLOCK END ####


skip_readwise_update = False
highlights_database = "Highlights/Readwise_highlights_archive.sqlite3"
current_datetime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# create database and tables if not exist
if not os.path.isfile(highlights_database):
    createDatabase(highlights_database)

connection = sqlite3.connect(highlights_database)
cursor = connection.cursor()

last_updated = getLastRunDate(cursor)


if not skip_readwise_update:
    # readwise currently (as of Aug 2021) the following categories:
    # "books", "articles", "tweets", "supplementals", "podcasts"
    list_books = getItemsInCategory("books", last_updated)
    list_articles = getItemsInCategory("articles", last_updated)
    list_tweets = getItemsInCategory("tweets", last_updated)
    list_supplementals = getItemsInCategory("supplementals", last_updated)
    list_podcasts = getItemsInCategory("podcasts", last_updated)
    list_categories = [
        list_books,
        list_articles,
        list_tweets,
        list_supplementals,
        list_podcasts,
    ]

    # parse each item into the sqlite database
    item_ids = []
    for category in list_categories:
        for page_items in category:
            for item in page_items:
                cursor.execute(
                    """
                    INSERT INTO ItemIds (id, category, title, author, cover_url, readwise_page, source_url, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        category=excluded.category,
                        title=excluded.title,
                        author=excluded.author,
                        cover_url=excluded.cover_url,
                        readwise_page=excluded.readwise_page,
                        source_url=excluded.source_url,
                        tags=excluded.tags;
                    """,
                    (
                        int(item["id"]),
                        item["category"],
                        item["title"],
                        item["author"],
                        item["cover_image_url"],
                        item["highlights_url"],
                        item["source_url"],
                        getTags(item["tags"]),
                    ),
                )

                connection.commit()
                # add each item id to a list so that we can only request highlights about updated items not about the rest
                item_ids = item_ids.append(item["id"])

    for item_id in item_ids:
        # get all highlights from article/podcast/etc. of id 'item_id'
        item_highlight_pages = getHighlightsInItem(item_id, last_updated)

        # iterate over each of the highlights
        for page in item_highlight_pages:
            for highlight in page:
                cursor.execute(
                    """
                    INSERT INTO Highlights (id, text, note, location, location_type, updated, highlighted_at, url, color, book_id, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        text=excluded.text,
                        note=excluded.note,
                        location=excluded.location,
                        location_type=excluded.location_type,
                        updated=excluded.updated,
                        highlighted_at=excluded.highlighted_at,
                        url=excluded.url,
                        color=excluded.color,
                        book_id=excluded.book_id,
                        tags=excluded.tags;
                    """,
                    (
                        highlight["id"],
                        highlight["text"],
                        highlight["note"],
                        highlight["location"],
                        highlight["location_type"],
                        highlight["updated"],
                        highlight["highlighted_at"],
                        highlight["url"],
                        highlight["color"],
                        highlight["book_id"],
                        getTags(item["tags"]),
                    ),
                )

                connection.commit()

    # Update Log table in sqlite with the current date/time string so next time
    # its run it can get only the new highlights and not re-request everything
    cursor.execute("Insert into Log values (NULL, ?)", ([current_datetime]))
    connection.commit()

