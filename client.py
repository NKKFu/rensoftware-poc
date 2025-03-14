"""
This script fetches products from a database and updates them in an API.
"""
import json
import os
import pyodbc
import requests
import logging
from logging.handlers import RotatingFileHandler

# Configure logging
handler = RotatingFileHandler('logs.log', maxBytes=5*1024*1024, backupCount=1)
logging.basicConfig(
    handlers=[handler],
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s:%(message)s',
)

# Database connection settings
# Read from settings.json
if not os.path.exists("settings.json"):
    logging.error("The settings.json file does not exist.")
    raise FileNotFoundError("The settings.json file does not exist.")

with open("settings.json", encoding="utf-8") as f:
    settings = json.load(f)
    DB_CONNECTION_STRING = settings["DB_CONNECTION_STRING"]
    API_URL = settings["API_URL"]
    SQL_QUERY = settings["SQL_QUERY"]

def sanitize_row_column(column):
    """
    Sanitizes a row column.
    """
    return str(column) if column is not None else None

def fetch_products_from_db():
    """
    Fetches products from the database.
    """
    logging.info("Starting to fetch products from the database.")
    try:
        conn = pyodbc.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute(SQL_QUERY)

        products = [{
            "code": sanitize_row_column(row[0]),
            "stock": sanitize_row_column(row[1]),
            "description": sanitize_row_column(row[2]),
            "ean": sanitize_row_column(row[3]),
            "stockMin": sanitize_row_column(row[4]),
            "lastPurchase": sanitize_row_column(row[5]),
        } for row in cursor.fetchall()]

        conn.close()
        logging.info("Successfully fetched %d products from the database.", len(products))
        return products
    except Exception as e:
        logging.error("Error fetching products from database: %s", e)
        return []

def update_products_in_api(products):
    """
    Updates the products in the API.
    """
    logging.info("Starting to update products in the API.")
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            url=f"{API_URL}/products",
            data=json.dumps(products),
            headers=headers,
            timeout=5 * 60,
        )
        if response.ok:
            logging.info("Products updated successfully.")
            print("Products updated successfully.")
        else:
            logging.error("Error updating products: %s", response.text)
            print("Error updating products:", response.text)
    except Exception as e:
        logging.error("Exception occurred while updating products in API: %s", e)

def main():
    """
    Fetches products from the database and updates them in the API.
    """
    logging.info("Script started.")
    products = fetch_products_from_db()
    if products:
        update_products_in_api(products)
    else:
        logging.info("No products found in the database.")
        print("No products found in the database.")
    logging.info("Script finished.")

if __name__ == "__main__":
    main()
