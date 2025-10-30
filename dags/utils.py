import pandas as pd
import argparse
from datetime import datetime, timedelta
import re
import snowflake.connector
from dotenv import load_dotenv
import os
import pymongo
import sshtunnel
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import base64
import logging
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient

# -------------------------------------------------------------------
# Utility Functions
# -------------------------------------------------------------------
load_dotenv()

# -------------------------------------------------------------------
# S&P 500 Tickers
# -------------------------------------------------------------------
def get_sp500_tickers() -> list[str]:
    """
    Fetches the current list of S&P 500 tickers from Wikipedia.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # Add a User-Agent so Wikipedia doesn't block the request
    dfs = pd.read_html(
        url,
        match="Symbol",
        storage_options={"User-Agent": "Mozilla/5.0"}
    )
    df = dfs[0]

    tickers = (
        df["Symbol"]
        .astype(str)
        .str.strip()
        .dropna()
        .drop_duplicates()
        .apply(lambda t: re.sub(r"\.", "-", t))  # Replace dots with dashes
        .sort_values()  # Sort alphabetically
        .tolist()
    )
    return tickers

# -------------------------------------------------------------------
# Date Utilities
# -------------------------------------------------------------------
# Get target date from command line arguments
def get_target_date():
    """
    Returns the target date string (YYYY-MM-DD) from command line args.
    Defaults to yesterday if not provided or invalid.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', help='Run for specific date (YYYY-MM-DD)')

    # In Jupyter, ignore extra arguments (like `--f`)
    args, unknown = parser.parse_known_args()

    # Try to parse the date or default to yesterday
    try:
        if args.date:
            # Validate format
            datetime.strptime(args.date, "%Y-%m-%d")
            return args.date
    except Exception:
        print(f"[WARN] Invalid --date format, defaulting to yesterday.")

    # Default fallback
    target = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[INFO] No valid --date provided. Defaulting to: {target}")
    return target

# -------------------------------------------------------------------
# Check if a date is a weekend
# -------------------------------------------------------------------
def is_weekend(date_str: str) -> bool:
    """
    Return True if the date is Saturday or Sunday.
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.weekday() >= 5  # 5 = Saturday, 6 = Sunday

from pathlib import Path

# -------------------------------------------------------------------
# Log Failed Fetches
# -------------------------------------------------------------------
def log_failed_fetch(symbol, date, reason):
    """
    Logs failed fetch attempts to a file for later review.
    """
    log_file = Path("logs/failed_fetches.txt")
    log_file.parent.mkdir(exist_ok=True)  # Create 'logs' folder if needed

    with log_file.open("a") as f:
        f.write(f"{symbol},{date},{reason}\n")

# -------------------------------------------------------------------
# Snowflake_password
# -------------------------------------------------------------------
def get_snowflake_connection_pw(schema: str = None):
    """
    Returns a Snowflake connection.
    - schema: override default schema if needed
    """
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "RAW_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "RAW"),
        schema=schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )

# -------------------------------------------------------------------
# Snowflake_keypair
# -------------------------------------------------------------------
def get_snowflake_connection(schema: str = None):
    """
    Connect to Snowflake using ONLY key-pair authentication.
    Requires one of:
      - SNOWFLAKE_PRIVATE_KEY_PATH
      - SNOWFLAKE_PRIVATE_KEY_B64
    Optional:
      - SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    """
    common = dict(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "RAW_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SNOWBEARAIR_DB"),
        schema=schema or os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    key_b64  = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
    key_pass = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    if not key_path and not key_b64:
        raise ValueError(
            "❌ Must set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PRIVATE_KEY_B64 in environment"
        )

    if key_path:
        with open(key_path, "rb") as f:
            key_pem = f.read()
    else:
        key_pem = base64.b64decode(key_b64)

    private_key = serialization.load_pem_private_key(
        key_pem,
        password=(key_pass.encode() if key_pass else None),
        backend=default_backend(),
    ).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(private_key=private_key, **common)

# -------------------------------------------------------------------
# SSH Tunnel
# -------------------------------------------------------------------
def get_ssh_tunnel():
    """
    Returns an active SSH tunnel context for MongoDB connections.
    Usage:
        with get_ssh_tunnel() as tunnel:
            client = get_mongo_client(tunnel.local_bind_port)
    """
    return sshtunnel.SSHTunnelForwarder(
        (os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT", 22))),
        ssh_username=os.getenv("SSH_USER"),
        ssh_password=os.getenv("SSH_PASSWORD"),
        remote_bind_address=(os.getenv("MONGO_HOST"), int(os.getenv("MONGO_PORT", 27017))),
    )

# -------------------------------------------------------------------
# MongoDB
# -------------------------------------------------------------------
def get_mongo_client(local_port=None):
    """
    Returns a MongoDB client.
    - If using SSH tunnel, pass tunnel.local_bind_port as local_port.
    - If connecting directly, env vars are used.
    """
    host = "127.0.0.1" if local_port else os.getenv("MONGO_HOST")
    port = local_port or int(os.getenv("MONGO_PORT", 27017))
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    auth_db = os.getenv("MONGO_DB", "admin")

    uri = f"mongodb://{user}:{password}@{host}:{port}/{auth_db}"
    return pymongo.MongoClient(uri)

def get_mongo_collection(client):
    """
    Returns the default MongoDB collection based on env vars.
    """
    db_name = os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION")
    return client[db_name][collection_name]

# -------------------------------------------------------------------
# Finnhub API
# -------------------------------------------------------------------
def get_finnhub_api_key():
    """
    Returns the Finnhub API key from environment variables.
    """
    api_key = os.getenv("FINHUB_API_KEY")
    if not api_key:
        raise ValueError("❌ FINHUB_API_KEY not found in environment")
    return api_key

# ---------------------------------------------------------
# SSH Tunnel to MongoDB
# ---------------------------------------------------------
def create_ssh_tunnel():
    """Start an SSH tunnel and return the tunnel object."""
    SSH_HOST = os.getenv("SSH_HOST")
    SSH_PORT = int(os.getenv("SSH_PORT"))
    SSH_USER = os.getenv("SSH_USER")
    SSH_PASSWORD = os.getenv("SSH_PASSWORD")
    MONGO_HOST = os.getenv("MONGO_HOST")
    MONGO_PORT = int(os.getenv("MONGO_PORT"))

    logging.info("Starting SSH tunnel to MongoDB...")
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=(MONGO_HOST, MONGO_PORT),
        local_bind_address=("127.0.0.1", 27017)
    )
    tunnel.start()
    logging.info(f"SSH tunnel established on local port {tunnel.local_bind_port}")
    return tunnel


# ---------------------------------------------------------
# MongoDB Client
# ---------------------------------------------------------
def get_mongo_client_cloud(tunnel):
    """Create a MongoDB client using an active SSH tunnel."""
    MONGO_USER = os.getenv("MONGO_USER")
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
    MONGO_DB_NAME = os.getenv("MONGO_DB")

    uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}"
        f"@127.0.0.1:{tunnel.local_bind_port}/"
        f"?authSource={MONGO_DB_NAME}"
    )
    client = MongoClient(uri)
    logging.info("MongoDB client connected successfully.")
    return client

# ---------------------------------------------------------
# Weather Record Builder
# ---------------------------------------------------------

def build_weather_record(weather_dict, target_date, city):
    """Extract weather metrics safely from API response."""
    daily_data = weather_dict.get("daily", {})
    return {
        "date": target_date,
        "city": city,
        "max_temp": daily_data.get("temperature_2m_max", [None])[0],
        "min_temp": daily_data.get("temperature_2m_min", [None])[0],
        "precip": daily_data.get("precipitation_sum", [None])[0],
        "max_wind": daily_data.get("windspeed_10m_max", [None])[0],
    }

# ---------------------------------------------------------
# Snowflake MERGE SQL Builder
# ---------------------------------------------------------

def build_merge_sql(rec, table):
    """Return a parameterized Snowflake MERGE statement."""
    return f"""
        MERGE INTO {table} t
        USING (SELECT
            '{rec["date"]}' AS DATE,
            '{rec["city"]}' AS CITY,
            {rec["max_temp"] if rec["max_temp"] is not None else 'NULL'} AS MAX_TEMP,
            {rec["min_temp"] if rec["min_temp"] is not None else 'NULL'} AS MIN_TEMP,
            {rec["max_wind"] if rec["max_wind"] is not None else 'NULL'} AS MAX_WIND,
            {rec["precip"] if rec["precip"] is not None else 'NULL'} AS PRECIP
        ) s
        ON t.DATE = s.DATE AND t.CITY = s.CITY
        WHEN MATCHED THEN UPDATE SET
            MAX_TEMP = s.MAX_TEMP,
            MIN_TEMP = s.MIN_TEMP,
            MAX_WIND = s.MAX_WIND,
            PRECIP   = s.PRECIP
        WHEN NOT MATCHED THEN INSERT
            (DATE, CITY, MAX_TEMP, MIN_TEMP, MAX_WIND, PRECIP)
        VALUES
            (s.DATE, s.CITY, s.MAX_TEMP, s.MIN_TEMP, s.MAX_WIND, s.PRECIP);
    """