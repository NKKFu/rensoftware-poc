"""
This script fetches products from a database and updates them in an API.
"""
import json
import os
import pyodbc
import requests


# Database connection settings
# Read from settings.json
if not os.path.exists("settings.json"):
    raise FileNotFoundError("The settings.json file does not exist.")

with open("settings.json", encoding="utf-8") as f:
    settings = json.load(f)
    DB_CONNECTION_STRING = settings["DB_CONNECTION_STRING"]
    API_URL = settings["API_URL"]
    SQL_QUERY = settings["SQL_QUERY"]

def fetch_products_from_db():
    """
    Fetches products from the database.
    """
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute(SQL_QUERY)
    products = [{
        "code": str(row[0]),
        "stock": str(row[1]),
        "description": str(row[2]),
        "ean": str(row[3]),
        "stockMin": str(row[4]),
    } for row in cursor.fetchall()]

    conn.close()
    return products

def update_products_in_api(products):
    """
    Updates the products in the API.
    """
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        url=f"{API_URL}/products",
        data=json.dumps(products),
        headers=headers,
        timeout=5 * 60,
    )
    if response.ok:
        print("Products updated successfully.")
    else:
        print("Error updating products:", response.text)

def main():
    """
    Fetches products from the database and updates them in the API.
    """
    products = fetch_products_from_db()
    if products:
        update_products_in_api(products)
    else:
        print("No products found in the database.")

if __name__ == "__main__":
    main()
