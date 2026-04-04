import sqlite3

DB_PATH = "/opt/airflow/data/finance.db"


def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Table 1: exchange rates (raw data)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS exchange_rates (
        date TEXT,
        currency TEXT,
        rate REAL
    )
    """)

    # Table 2: metrics (processed data)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS currency_metrics (
        date TEXT,
        strongest_currency TEXT,
        strongest_rate REAL,
        weakest_currency TEXT,
        weakest_rate REAL
    )
    """)

    conn.commit()
    conn.close()


def insert_transactions(data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT INTO exchange_rates VALUES (?, ?, ?)",
        [(d["date"], d["currency"], d["rate"]) for d in data]
    )

    conn.commit()
    conn.close()


def insert_metrics(data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT INTO currency_metrics VALUES (?, ?, ?, ?, ?)",
        [
            (
                d["date"],
                d["strongest_currency"],
                d["strongest_rate"],
                d["weakest_currency"],
                d["weakest_rate"],
            )
            for d in data
        ]
    )

    conn.commit()
    conn.close()