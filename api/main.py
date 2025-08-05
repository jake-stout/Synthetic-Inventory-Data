import os
from datetime import datetime
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from fastapi import FastAPI, Query

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "54321"))
DB_NAME = os.getenv("DB_NAME", "inventory")
DB_USER = os.getenv("DB_USER", "inventory")
DB_PASSWORD = os.getenv("DB_PASSWORD", "inventory")

app = FastAPI()


pool: Optional[SimpleConnectionPool] = None


@app.on_event("startup")
def startup() -> None:
    global pool
    pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


@app.on_event("shutdown")
def shutdown() -> None:
    if pool:
        pool.closeall()


@app.get("/transactions")
def read_transactions(
    warehouse: Optional[str] = Query(default=None),
    start_date: Optional[datetime] = Query(default=None),
):
    assert pool is not None
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM streamed_transactions"
            params: List[object] = []
            conditions: List[str] = []
            if warehouse:
                conditions.append("warehouse = %s")
                params.append(warehouse)
            if start_date:
                conditions.append("transaction_timestamp >= %s")
                params.append(start_date)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY transaction_timestamp DESC LIMIT 100"
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        pool.putconn(conn)
