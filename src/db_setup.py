import sqlite3

DB_FILE = 'db/aqi.db'

def create_table():
    """Creates the air_quality table in the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS air_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            aqi INTEGER,
            o3 REAL,
            co REAL,
            so2 REAL,
            pm25 REAL,
            pm10 REAL,
            timestamp TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()
    print("Database table 'air_quality' created successfully.")

if __name__ == "__main__":
    create_table()