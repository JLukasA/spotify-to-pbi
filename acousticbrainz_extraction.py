import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import datetime
import sqlite3
import requests
import time

AB_API_URL = "https://acousticbrainz.org/api/v1/"


def get_missing_isrc(db_loc: str) -> list:
    """ Returns a list containing ISRC of songs in the spotify table that is not in the acousticbrainz table. """
    with sqlite3.connect(db_loc) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT isrc FROM raw_spotify_data")
        spotify_isrc = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT DISTINCT isrc FROM raw_acousticbrainz_data")
        acousticbrainz_isrc = {row[0] for row in cursor.fetchall()}
        new_isrc = spotify_isrc - acousticbrainz_isrc
        return list(new_isrc)


def isrc_to_mbid(isrc_list: list) -> list:
    """ Fetch mbid from musicbrainz to use to fetch data from acousticbrainz. Rate limit 300 requests per second. """
    mbid_list = []
    for isrc in isrc_list:
        url = f"https://musicbrainz.org/ws/2/recording/?query=isrc:{isrc}&fmt=json"
        res = requests.get(url)

        if res.status_code == 200:
            data = res.json()
            if data.get("recordings"):
                mbid_list.append(data["recordings"][0]["id"])
            else:
                mbid_list.append(None)
        if res.status_code == 429:
            # TODO: jump back up to start of this loop after sleeping
            print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
            time.sleep(1)

    return mbid_list


def extract_data(mbid_list: str):
    """ Extract high- and low-level metadata about Spotify tracks using the acousticbrainz API. Rate limit 10 requests per 10 seconds. """

    ab_data_high = {}
    ab_data_low = {}
    delay = 1
    request_counter = 1
    start_time = time.time()
    for mbid in mbid_list:
        # extract high-level data
        url_high = f"{AB_API_URL}{mbid}/high-level"
        res_high = requests.get(url_high)
        if res_high.status_code == 200:
            ab_data_high[mbid] = res_high.json()
        elif res_high.status_code == 429:  # rate limit exceeded
            # TODO - if rate limit exceeded, sleep 10 sec and make new request? right now it's just sleeping, hit status code 429 so res_high.json() is no good
            # SAme true for res_low
            print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
            time.sleep(10)
            ab_data_low[mbid] = res_high.json()
        else:
            print(f"Failed fetching high-level data. Status code {res_high.status_code}")
            ab_data_high[mbid] = None

        # extract low-level data
        url_low = f"{AB_API_URL}{mbid}/low-level"
        res_low = requests.get(url_low)
        if res_low.status_code == 200:
            ab_data_low[mbid] = res_low.json()
        elif res_low.status_code == 429:
            print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
            time.sleep(10)
            ab_data_low[mbid] = res_low.json()
        else:
            print(f"Failed fetching low-level data. Status code {res_low.status_code}")
            ab_data_low[mbid] = None

    return ab_data_high, ab_data_low


def process_data(high_level_data: dict, low_level_data: dict) -> pd.DataFrame:

    data = {

    }
    df = pd.DataFrame(data)
    return df


def initialize_database(s: str):
    """ Initialize database if it doesn't exist. Needed for first run. """

    conn = sqlite3.connect(s)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_acousticbrainz_data (       
            played_at TEXT PRIMARY KEY,
            track_id,
            tempo,
            key,
            scale,
            mood,
            genre TEXT,
            mbid TEXT,
            isrc TEXT
            )
                   """)
    conn.commit()
    conn.close()


def upload_data(df: pd.DataFrame, db_loc):
    # Establish connection to database and initialize table if it doesn't exist
    s = db_loc.replace('sqlite:///', '')
    engine = create_engine(db_loc)
    conn = sqlite3.connect(s)
    print(f"Connected to database {s}.")
    cursor = conn.cursor()
    initialize_database(s)


def run(db_loc):
    isrc = get_missing_isrc(db_loc)
    mbid = isrc_to_mbid(isrc)
    high, low = extract_data(mbid)
    df = process_data(high, low)
    upload_data(df, db_loc)
