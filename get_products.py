import os

import psycopg2
import requests
from psycopg2 import OperationalError
import math
from dotenv import load_dotenv


load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

headers = {
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'content-type': 'application/json',
            'x-requested-with': 'detmir-ui',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/103.0.0.0 Safari/537.36',
            'sec-ch-ua-platform': '"Windows"',
            'Accept': '*/*',
            'Origin': 'https://www.detmir.ru',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://www.detmir.ru/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru-RU,ru;q=0.9'

        }


def save_products(page, category_id):
    for product in page['items']:
        item_id = product['productId']
        title = product['title']
        address = product['vendor']['address']
        price = product['old_price']['price'] if product['old_price'] else product['price']['price']
        discount_price = product['price']['price']
        url = product['link']['web_url']
        if price == discount_price:
            discount_price = None

        category = [
            (item_id, title, address, price, discount_price, url, category_id)
        ]

        category_records = ", ".join(["%s"] * len(category))
        insert_query = (
            f"INSERT INTO product (item_id, title, address, price, discount_price, url, category_id) VALUES    {category_records}"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, category)


def get_category_products(item_id, category_id):
    offset = 0
    page_number = 1
    params = {
        'filter': f'categories[].alias:{"accessories_car_seats"};promo:false;withregion:RU-MOW',
        'expand': 'meta.facet.ages.adults,meta.facet.gender.adults,webp',
        'meta': '*',
        'limit': 30,
        'offset': offset,
        'sort': 'popularity:desc'
    }
    api_products = requests.get('https://api.detmir.ru/v2/products', params=params, headers=headers)
    save_products(api_products.json(), category_id)
    page_count = math.ceil(api_products.json()['meta']['length'] / 30)
    page_number += 1
    while page_number <= page_count:
        offset += 30
        params = {
            'filter': f'categories[].alias:metallicheskie_konstruktory;promo:false;withregion:RU-MOW',
            'expand': 'meta.facet.ages.adults,meta.facet.gender.adults,webp',
            'meta': '*',
            'limit': 30,
            'offset': offset,
            'sort': 'popularity:desc'
        }
        api_products = requests.get('https://api.detmir.ru/v2/products', params=params, headers=headers)
        save_products(api_products.json(), category_id)
        page_number += 1


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def create_connection(db_name, db_user, db_host, db_port):
    a = psycopg2.connect(
        database=db_name,
        user=db_user,
        host=db_host,
        port=db_port,
    )
    print("Connection to PostgreSQL DB successful")
    return a


def execute_read_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


connection = create_connection(
    DB_NAME, DB_USER, DB_HOST, DB_PORT
)
create_product_table = """
CREATE TABLE IF NOT EXISTS product (
  id SERIAL PRIMARY KEY,
  item_id TEXT UNIQUE,
  title TEXT NOT NULL, 
  address TEXT NOT NULL,
  price DECIMAL,
  discount_price DECIMAL,
  url TEXT NOT NULL,
  category_id TEXT REFERENCES category(item_id)
)
"""
execute_query(connection, create_product_table)

select_users = "SELECT * FROM category WHERE category.level = 3"
users = execute_read_query(connection, select_users)

for user in reversed(users):
    arr_url = user[3].split('/')[-2]
    item_id = arr_url
    print(get_category_products(item_id, user[2]))

