#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import requests

# getting highlights from a particular book
# made after February 1st, 2020, 21:35:53 UTC
querystring = {
    "book_id": 6992375,
}

if not "API_TOKEN_READWISE" in os.environ:
    raise SystemExit("Error: Environment variable API_TOKEN_READWISE is not set")

response = requests.get(
    url="https://readwise.io/api/v2/highlights/",
    headers={"Authorization": os.environ["API_TOKEN_READWISE"]},
    params=querystring,
)

data = response.json()
print(data)
