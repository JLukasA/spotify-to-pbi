import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import datetime
import sqlite3
import requests
import time

AB_API_URL = "https://acousticbrainz.org/api/v1/"


def isrc_to_mbid(isrc_list: list) -> list:
    """ Fetch mbid from musicbrainz to use to fetch data from acousticbrainz. Rate limit 300 requests per second. """
    mbid_list = []
    for isrc in isrc_list:
        url = f"https://musicbrainz.org/ws/2/recording/?query=isrc:{isrc}&fmt=json"
        res = requests.get(url)
        data = res.json()

        if data.get("recordings"):
            mbid_list.append(data["recordings"][0]["id"])
        else:
            mbid_list.append(None)
    return mbid_list


def extract_data(mbid_list: str):
    """ Extract high- and low-level information about Spotify tracks using the acousticbrainz API. Rate limit 10 requests per 10 seconds. """

    ab_data_high = {}
    ab_data_low = {}

    delay = 1
    request_counter = 1
    start_time = time.time()
    for mbid in mbid_list:
        # extract high-level data
        url_high = f"{AB_API_URL}{mbid}/high-level"
        res_high = requests.get(url_high)
        request_counter += 1
        if res_high.status_code == 200:
            ab_data_high[mbid] = res_high.json()
        else:
            print(f"Failed fetching high-level data. Status code {res_high.status_code}")
            ab_data_high[mbid] = None
        time.sleep(delay)

        # extract low-level data
        url_low = f"{AB_API_URL}{mbid}/low-level"
        res_low = requests.get(url_low)
        request_counter += 1
        if res_low.status_code == 200:
            ab_data_low[mbid] = res_low.json()
        else:
            print(f"Failed fetching low-level data. Status code {res_low.status_code}")
            ab_data_low[mbid] = None

        time.sleep(delay)

        # ensure rate limit is not exceeded
        if request_counter >= 10:
            time_spent = time.time()-start_time
            if time_spent < 10:
                time.sleep(10.01-time_spent)
            request_counter = 0
            start_time = time.time()

    return ab_data_high, ab_data_low


def process_data(high_level_data: dict, low_level_data: dict) -> pd.DataFrame:
    df = pd.DataFrame
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
            genre
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


def run(db_loc, song_df: pd.DataFrame):
    # song df har isrc
    # acousticbrainz
    extract_data(song_df)
    process_data()
    upload_data(db_loc)
