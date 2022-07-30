import os
import re
import uuid

from psycopg2 import OperationalError
import psycopg2
from dotenv import load_dotenv

import dirtyjson
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

load_dotenv()

s = requests.Session()
retry = Retry(connect=5, backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry)
s.mount('http://', adapter)
s.keep_alive = False

headers = {
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                          ' (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/'
                      'apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru-RU,ru;q=0.9',
            'Upgrade-Insecure-Requests': '1'
        }

base_url = 'https://www.detmir.ru'

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


def create_connection(db_name, db_user, db_host, db_port):
    a = psycopg2.connect(
        database=db_name,
        user=db_user,
        host=db_host,
        port=db_port,
    )
    print("Connection to PostgreSQL DB successful")
    return a


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


create_category_table = """
CREATE TABLE IF NOT EXISTS category (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL, 
  item_id TEXT UNIQUE,
  url TEXT NOT NULL,    
  level INTEGER,
  parent_category TEXT REFERENCES category(item_id)
)
"""
connection = create_connection(
    DB_NAME, DB_USER, DB_HOST, DB_PORT
)
execute_query(connection, create_category_table)


def get_absolute_url(url):
    return base_url + url


def get_categories():
    base_page = s.get('https://www.detmir.ru/', headers=headers).text
    base_json = re.findall('" id="app-cached-data">(.*)</script></div><script type="text/template', base_page)[0]
    base_page = base_json.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>') \
        .replace('&quot;', '"').replace("&#39;", "'")
    categories = dirtyjson.loads(base_page)

    for l1_xp in categories["menus"]["data"]["main"]["items"]:
        if not l1_xp['title'] or l1_xp['title'] == 'Акции':
            continue
        department_name = l1_xp['title']
        department_url = get_absolute_url(l1_xp['url'])
        item_id_l1_xp = uuid.uuid4().__str__()

        category = [
            (item_id_l1_xp, department_name, department_url, 1, None)
        ]

        category_records = ", ".join(["%s"] * len(category))

        insert_query = (
            f"INSERT INTO category (item_id, title, url, level, parent_category) VALUES {category_records}"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, category)

        # LEVEL 2
        families_xp = l1_xp.get('items', []) if l1_xp.get('items', []) else []

        for family_xp in families_xp:  # first li is a back arrow (Regresar)
            if not family_xp['title']:
                continue
            item_id_family_xp = uuid.uuid4().__str__()
            family_name = family_xp['title']
            family_url = get_absolute_url(family_xp['url'])
            category = [
                (item_id_family_xp, family_name, family_url, 2, item_id_l1_xp)
            ]

            category_records = ", ".join(["%s"] * len(category))

            insert_query = (
                f"INSERT INTO category (item_id, title, url, level, parent_category) VALUES {category_records}"
            )
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(insert_query, category)

            # LEVEL 3
            lines_xp = family_xp.get('items', []) if family_xp.get('items', []) else []

            for line_xp in lines_xp:
                if not line_xp['title']:
                    continue
                item_id_line_xp = uuid.uuid4().__str__()
                category_name = line_xp['title']
                cat_url = line_xp['url']
                if 'zoozavr' not in cat_url:
                    cat_url = get_absolute_url(line_xp['url'])
                category = [
                    (item_id_line_xp, category_name, cat_url, 3, item_id_family_xp)
                ]

                category_records = ", ".join(["%s"] * len(category))

                insert_query = (
                    f"INSERT INTO category (item_id, title, url, level, parent_category) VALUES {category_records}"
                )
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute(insert_query, category)


print(get_categories())
