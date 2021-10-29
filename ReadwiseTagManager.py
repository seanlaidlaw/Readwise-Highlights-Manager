#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import csv
import time
import requests
import sqlite3
import math
from datetime import datetime

if not "API_TOKEN_READWISE" in os.environ:
    raise SystemExit(
        "Error: Environment variable API_TOKEN_READWISE is not set")


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
    Get and format tags for a given highlight or article
    sqlite doesn't support arrays so we are going to input it as
    space separated string of tag ids

    Args:
        tag_list: array of tags to convert to string
    """

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
    
    Parses the data json response for the count value and divides it by the max
    page_size we request (1000) to work out how many pages are needed.
    As the API requires we give the requested page of Highlight results as an
    integer we round it up using the math.ceil function

    Args:
        data: data json response from API query
    """

    total_pages = 0
    if "count" in data:
        # round up the count / 1000 to get the number of pages
        # needed to request if we have 1000 results per page
        total_pages = int(math.ceil(float(data["count"]) / float(1000)))
    return total_pages


def getUpdatedHighlights(updated_filter):
    """
    Returns a list of highlights that are modified or added after updated_filter.

    Args:
        updated_filter: datetime string
    """
    pages_of_results = []
    page_number = 1

    # get total number of highlights by writing a small request to see the total count.
    # We are interested in the updated but not new highlights, so we set filter of updated
    # since and highlighted before
    response = requests.get(
        url="https://readwise.io/api/v2/highlights/",
        headers={
            "Authorization": os.environ["API_TOKEN_READWISE"],
        },
        params={
            "page_size": 1,
            "page": 1,
            "updated__gt": updated_filter,
            "highlighted_at__lt": updated_filter,
        },
    )

    data = response.json()
    total_pages = getTotalPagesOutput(data)

    # for each of the pages we need to request, make a request and append its results to a list
    while page_number <= total_pages:
        querystring = {
            "page_size": 1000,
            "page": page_number,
            "updated__gt": updated_filter,
            "highlighted_at__lt": updated_filter,
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

    return pages_of_results


def getItemsInCategory(category, updated_filter):
    """
    Get all items in a category.

    Args:
        category: category of highlight (str) e.g. "podcast", "book", "article"
        updated_filter: datetime string 
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
        item_id: id of item containing highlights
        updated_filter: datetime string
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
    Returns list of all item ids from database.

    Args:
        database_cursor: cursor for highlight database connection
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
    Returns list of item ids that have the given tag.

    Args:
        database_cursor: cursor for highlight database connection
        tag_id: id of tag with which to filter items
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
    Get the last run date.

    Args:
        database_cursor: cursor for highlight database connection
    """
    database_cursor.execute(
        "SELECT last_updated FROM log ORDER by id DESC LIMIT 1;")
    data = database_cursor.fetchall()

    # if not run before then set last_date to start of UNIX time to get all readwise highlights
    last_date = "1970-01-01T00:00:00Z"

    if len(data) > 0:
        for last_dates in data:
            # we only request one column but it returns list
            last_date = last_dates[0]

    return last_date


def exportSQLiteCSV(database_cursor, filename):
    """
    Exports the data from the SQLite database cursor to CSV.

    Args:
        database_cursor: cursor for highlight database connection
        filename: string for filename of exported CSV file
    """
    database_cursor.execute(
        "SELECT * FROM Highlights LEFT JOIN ItemIds ON Highlights.book_id=ItemIds.id"
    )
    data = database_cursor.fetchall()

    with open(filename, "w") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data)


def UpdateMissingHighlightsTags(database_cursor):
    """
    Updates tags of all highlights that are missing from TSV.

    Args:
        database_cursor: cursor for highlight database connection
    """
    tsv_data = pd.read_csv("Data/addon_tags.tsv", sep="\t")

    # get each highlight and its notes containing tags
    database_cursor.execute(
        "SELECT id AS highlight_id,note FROM Highlights WHERE note LIKE '.%';"
    )
    data = database_cursor.fetchall()

    for highlight in data:
        highlight_id = highlight[0]
        highlight_tags = highlight[1]

        # make a set of just the tags of the highlight
        only_tag_set = set()
        for tag_set in highlight_tags.split(" "):
            if tag_set.startswith("."):
                only_tag_set.add(tag_set)

        # see if tag is in TSV to be associated with another tag
        for tag in only_tag_set:
            if tag in tsv_data["primary_tag"].tolist():
                subset = tsv_data.loc[tsv_data["primary_tag"] == tag]
                associated_tag = subset["associated_tag"].tolist()[0]

                if not associated_tag in only_tag_set:
                    addTagToHighlight(highlight_id, associated_tag)


def addTagToHighlight(highlight_id, tag):
    """
    Add a tag to a highlight.

    Args:
        highlight_id: id of highlight to add tag to
        tag: string for name of tag
    """
    response = requests.get(
        url="https://readwise.io/api/v2/highlights/{}".format(highlight_id),
        headers={
            "Authorization": os.environ["API_TOKEN_READWISE"],
        },
    )
    data = response.json()
    current_note = data["note"]

    new_note = current_note + " " + tag
    response = requests.patch(
        url="https://readwise.io/api/v2/highlights/{}".format(highlight_id),
        headers={
            "Authorization": os.environ["API_TOKEN_READWISE"],
        },
        data={
            "note": new_note,
        },
    )
    data = response.json()




# get list of highlights that are not new but which have been updated since last run
list_only_updated = getUpdatedHighlights(last_updated)


def updateLocalDatabase(cursor, connection, last_updated):
    """
    Updates the local database with the latest highlights from Readwise.

    Args:
        cursor: cursor for highlight database connection
        connection: highlight database connection object
        last_updated: datetime string to filter out old unmodified highlights
    """

    # get list of highlights that are not new but which have been updated since last run
    list_only_updated = getUpdatedHighlights(last_updated)

    # for each updated highlight replace its entry in the database
    for page_items in list_only_updated:
        for item in page_items:
            cursor.execute(
                """
                    INSERT OR REPLACE INTO Highlights (id, text, note, location, location_type, updated, highlighted_at, url, color, book_id, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                (
                    item["id"],
                    item["text"],
                    item["note"],
                    item["location"],
                    item["location_type"],
                    item["updated"],
                    item["highlighted_at"],
                    item["url"],
                    item["color"],
                    item["book_id"],
                    getTags(item["tags"]),
                ),
            )

            connection.commit()

    # now get all new highlights
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
                    INSERT OR REPLACE INTO ItemIds (id, category, title, author, cover_url, readwise_page, source_url, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
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
                item_ids.append(int(item["id"]))

    # if after going through each of the categories there are no new highlights then exit function early
    if len(item_ids) < 1:
        print("No new highlights to sync")
        return

    for item_id in item_ids:
        # get all highlights from article/podcast/etc. of id 'item_id'
        item_highlight_pages = getHighlightsInItem(item_id, last_updated)

        # iterate over each of the highlights
        for page in item_highlight_pages:
            for highlight in page:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO Highlights (id, text, note, location, location_type, updated, highlighted_at, url, color, book_id, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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

    # after writing to database export to CSV
    exportSQLiteCSV(cursor, "Highlights/readwise_higlights_export.csv")

#### FUNCTION BLOCK END ####


highlights_database = "Highlights/Readwise_highlights_archive.sqlite3"
current_datetime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# create database and tables if not exist
if not os.path.isfile(highlights_database):
    createDatabase(highlights_database)

connection = sqlite3.connect(highlights_database)
cursor = connection.cursor()

last_updated = getLastRunDate(cursor)
updateLocalDatabase(cursor, connection, last_updated)


UpdateMissingHighlightsTags(cursor)

