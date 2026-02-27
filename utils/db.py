
# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path
from contextlib import contextmanager
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'db' / 'app.db'

DDL_RAW = """
CREATE TABLE IF NOT EXISTS raw_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,
    category TEXT,
    title TEXT,
    price_raw TEXT,
    address_raw TEXT,
    image_url TEXT,
    link TEXT UNIQUE,
    page INTEGER,
    scraped_at TEXT DEFAULT (CURRENT_TIMESTAMP)
);
"""

@contextmanager
def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.execute(DDL_RAW)
        yield conn
        conn.commit()
    finally:
        conn.close()

def fetch_all_raw():
    """Retourne un DataFrame (si pandas dispo) trié par id DESC, sinon None."""
    with connect_db() as conn:
        try:
            import pandas as pd
            return pd.read_sql_query('SELECT * FROM raw_listings ORDER BY id DESC', conn)
        except Exception:
            return None

def insert_raw_many(rows: List[Dict[str, Any]]) -> int:
    """
    INSERT OR IGNORE (fallback insert-only).
    """
    if not rows:
        return 0
    if not isinstance(rows, list):
        raise TypeError(f"insert_raw_many: rows must be List[Dict], got {type(rows).__name__}")
    inserted = 0
    with connect_db() as conn:
        cur = conn.cursor()
        for r in rows:
            if not isinstance(r, dict):
                raise TypeError(f"insert_raw_many: each row must be Dict, got {type(r).__name__}")
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO raw_listings "
                    "(source, category, title, price_raw, address_raw, image_url, link, page) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (
                        r.get('source'), r.get('category'), r.get('title'), r.get('price_raw'),
                        r.get('address_raw'), r.get('image_url'), r.get('link'), r.get('page')
                    )
                )
                inserted += cur.rowcount
            except Exception:
                # on ignore la ligne fautive et on continue
                pass
        conn.commit()
    return inserted

def upsert_raw_many_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    UPSERT demandé : écraser si existe.
    Stratégie : UPDATE d’abord ; si 0 ligne affectée -> INSERT.
    Retourne {'inserted': X, 'updated': Y, 'errors': Z}
    """
    if not rows:
        return {'inserted': 0, 'updated': 0, 'errors': 0}
    if not isinstance(rows, list):
        raise TypeError(f"upsert_raw_many_counts: rows must be List[Dict], got {type(rows).__name__}")

    ins = upd = err = 0
    with connect_db() as conn:
        cur = conn.cursor()
        for r in rows:
            if not isinstance(r, dict):
                raise TypeError(f"upsert_raw_many_counts: each row must be Dict, got {type(r).__name__}")
            try:
                link = r.get('link')
                if not link:
                    continue

                # 1) UPDATE (écrasement)
                cur.execute(
                    """UPDATE raw_listings SET
                         source=?,
                         category=?,
                         title=?,
                         price_raw=?,
                         address_raw=?,
                         image_url=?,
                         page=?,
                         scraped_at=CURRENT_TIMESTAMP
                       WHERE link=?""",
                    (
                        r.get('source'), r.get('category'), r.get('title'), r.get('price_raw'),
                        r.get('address_raw'), r.get('image_url'), r.get('page'), link
                    )
                )
                if cur.rowcount == 1:
                    upd += 1
                    continue

                # 2) INSERT si absent
                cur.execute(
                    """INSERT INTO raw_listings
                         (source, category, title, price_raw, address_raw, image_url, link, page, scraped_at)
                       VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
                    (
                        r.get('source'), r.get('category'), r.get('title'), r.get('price_raw'),
                        r.get('address_raw'), r.get('image_url'), link, r.get('page')
                    )
                )
                ins += 1

            except Exception:
                err += 1
        conn.commit()
    return {'inserted': ins, 'updated': upd, 'errors': err}
