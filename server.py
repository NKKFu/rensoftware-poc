from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import sqlite3


app = FastAPI(docs_url=None, redoc_url=None)

# Database setup
def init_db():
    conn = sqlite3.connect("products.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            code TEXT PRIMARY KEY,
            description TEXT,
            stock TEXT,
            stockMin TEXT,
            ean TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# Product model
class Product(BaseModel):
    code: str
    description: str
    stock: str
    stockMin: str
    ean: str

@app.post("/products")
def add_products(products: List[Product]):
    conn = sqlite3.connect("products.db")
    cursor = conn.cursor()

    for product in products:
        cursor.execute("""
            INSERT INTO products (code, description, stock, stockMin, ean) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET description=excluded.description, stock=excluded.stock, stockMin=excluded.stockMin, ean=excluded.ean
        """, (product.code, product.description, product.stock, product.stockMin, product.ean))

    conn.commit()

    cursor.execute("SELECT code, description, stock, stockMin, ean FROM products")
    all_products = [{
        "code": row[0],
        "description": row[1],
        "stock": row[2],
        "stockMin": row[3],
        "ean": row[4],
    } for row in cursor.fetchall()]

    conn.close()
    return all_products

@app.get("/products")
def get_products():
    conn = sqlite3.connect("products.db")
    cursor = conn.cursor()
    cursor.execute("SELECT code, description, stock, stockMin, ean FROM products")
    products = [{
        "code": row[0],
        "description": row[1],
        "stock": row[2],
        "stockMin": row[3],
        "ean": row[4],
    } for row in cursor.fetchall()]
    conn.close()
    return products

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
