import sqlite3
import sys

def create_table(db_file):
    """Creates the air_quality table in the specified SQLite database."""
    conn = sqlite3.connect(db_file)
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
    print(f"Database table 'air_quality' created in {db_file} successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python db_setup.py <database_file>")
    else:
        db_file = sys.argv[1]
        create_table(db_file)