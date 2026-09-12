"""
Shared PostgreSQL connection helper.

Reads connection details from environment variables (see .env.example),
defaulting to the values provided for local development:
    host=localhost, port=5432, database=rideshare, user=postgres
"""

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "rideshare")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

SQLALCHEMY_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """SQLAlchemy engine — used by the FastAPI read layer."""
    return create_engine(SQLALCHEMY_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)


@contextmanager
def get_connection():
    """Raw psycopg2 connection — used by the Spark foreachBatch writer."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
