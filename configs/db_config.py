# config/db_config.py
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

def get_connection(db_override=None):
    """
    Connect to PostgreSQL using environment variables.
    If db_override is provided, connect to that DB instead.
    """
    dbname = db_override if db_override else os.getenv("DB_NAME")

    return psycopg2.connect(
        dbname=dbname,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


def initialize_database():
    """
    Ensure the database exists, then run the schema file.
    """
    # Step 1: Connect to postgres DB to create the target DB if needed
    conn = get_connection(db_override="postgres")
    conn.autocommit = True
    cur = conn.cursor()

    db_name = os.getenv("DB_NAME")

    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (db_name,))
    exists = cur.fetchone()

    if not exists:
        print(f"Creating database {db_name}...")
        cur.execute(f"CREATE DATABASE {db_name};")
        conn.commit()
    else:
        print(f"Database {db_name} already exists.")

    cur.close()
    conn.close()

    # Step 2: Connect to new DB and run schema
    conn = get_connection()
    cur = conn.cursor()

    with open("db/schema.sql", "r") as f:
        schema_sql = f.read()
        cur.execute(schema_sql)

    conn.commit()
    cur.close()
    conn.close()


def initialize_medallion_schema():
    """
    Initialize the medallion architecture (Bronze, Silver, Gold layers).
    This should be run after initialize_database().
    """
    conn = get_connection()
    cur = conn.cursor()

    print("Initializing Medallion Architecture (Bronze, Silver, Gold layers)...")

    with open("db/medallion_schema.sql", "r") as f:
        medallion_sql = f.read()
        cur.execute(medallion_sql)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Medallion schema initialized successfully!")


def save_to_db(table_name, df):
    """
    Generic function to insert pandas DataFrame into a PostgreSQL table.

    Args:
        table_name (str): name of the target table.
        df (pd.DataFrame): pandas DataFrame
    """
    if df.empty:
        print(f"No data to insert into {table_name}.")
        return

    conn = get_connection()
    cur = conn.cursor()

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    execute_values(cur, insert_query, values)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(df)} rows into '{table_name}'.")
