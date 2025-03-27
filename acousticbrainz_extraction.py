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
        cursor.execute("SELECT isrc FROM raw_acousticbrainz_data")
        acousticbrainz_isrc = {row[0] for row in cursor.fetchall()}
        new_isrc = spotify_isrc - acousticbrainz_isrc
        return list(new_isrc)


def isrc_to_mbid(isrc_list: list) -> list:
    """ Fetch mbid from musicbrainz to use to fetch data from acousticbrainz. Rate limit 300 requests per second. """
    mbid_list = []
    for isrc in isrc_list:
        url = f"https://musicbrainz.org/ws/2/recording/?query=isrc:{isrc}&fmt=json"

        while True:
            res = requests.get(url)

            if res.status_code == 200:
                data = res.json()
                if data.get("recordings"):
                    mbid_list.append(data["recordings"][0]["id"])
                else:
                    print(f"No mbid data available for irsc {isrc}.")
                    mbid_list.append(None)
                break
            if res.status_code == 429:
                print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                time.sleep(1)
            else:
                print(f"Failed fetching mbid. Status code {res.status_code}.")
                mbid_list.append(None)
                break

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

        while True:
            res_high = requests.get(url_high)

            if res_high.status_code == 200:
                ab_data_high[mbid] = res_high.json()
                break
            elif res_high.status_code == 429:
                print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                time.sleep(10)
                ab_data_low[mbid] = res_high.json()
            else:
                print(f"Failed fetching high-level data. Status code {res_high.status_code}")
                ab_data_high[mbid] = None
                break

        # extract low-level data
        url_low = f"{AB_API_URL}{mbid}/low-level"

        while True:
            res_low = requests.get(url_low)
            if res_low.status_code == 200:
                ab_data_low[mbid] = res_low.json()
                break
            elif res_low.status_code == 429:
                print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                time.sleep(10)
                ab_data_low[mbid] = res_low.json()
            else:
                print(f"Failed fetching low-level data. Status code {res_low.status_code}")
                ab_data_low[mbid] = None
                break

    return ab_data_high, ab_data_low


def process_data(high_level_data: dict, low_level_data: dict, isrc_list: list, mbid_list: list) -> pd.DataFrame:

    data = []
    for isrc, mbid in zip(isrc_list, mbid_list):
        if not mbid:
            continue
        high = high_level_data.get(mbid, {}).get("highlevel", {})
        low = low_level_data.get(mbid, {}).get("lowlevel", {})
        features = {
            "isrc": isrc,
            "mbid": mbid,
            "tempo": low.get("bpm"),
            "danceability": high.get("danceability", {}).get("value"),
            "energy": high.get("energy", {}).get("value"),
            "instrumentality": high.get("voice_instrumental", {}).get("value"),
            "instrumentality_prob": high.get("voice_instrumental", {}).get("probability"),
            "gender": high.get("gender", {}).get("value"),
            "gender_prob": high.get("gender", {}).get("probability"),
            "intensity": high.get("arousal", {}).get("value"),
            "valence": high.get("valence", {}).get("value"),
            "timbre": high.get("timbre", {}).get("value"),
            "tonality": high.get("tonal_atonal", {}).get("value"),
            "genre": high.get("genre", {}).get("value"),
            "genre_prob": high.get("genre", {}).get("probability"),
            "mood": high.get("mood", {}).get("value"),
            "key": high.get("key", {}).get("key"),
        }

        data.append(features)

    df = pd.DataFrame(data)
    return df


def upload_data(df: pd.DataFrame, db_loc):
    # Establish connection to database and initialize table if it doesn't exist
    s = db_loc.replace('sqlite:///', '')
    engine = create_engine(engine)
    initialize_database(engine)
    print(f"Connected to database {s}.")

    data = pd.DataFrame()
    try:
        data.to_sql('raw_acousticbrainz_data', engine, index=False, if_exists='append')
    except Exception as e:
        print(f"failed to upload to database {s}. Error : {e}")
    finally:
        engine.dispose()


def initialize_database(engine):
    """ Initialize database if it doesn't exist. Needed for first run. """
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_acousticbrainz_data (
                isrc TEXT PRIMARY KEY NOT NULL,     -- International Standard Recording Code
                mbid TEXT UNIQUE,                   -- MusicBrainz ID, UUID format                       
                tempo REAL,                         -- BPM
                danceability TEXT,                  -- low/medium/high
                energy TEXT,                        -- low/medium/high
                instrumentality TEXT,               -- instrumental/voice
                instrumentality_prob REAL,          -- probability of being instrumental/voice
                gender TEXT,                        -- male/female
                gender_prob REAL,                   -- probability of male/female
                intensity TEXT,                     -- low/medium/high
                valence TEXT,                       -- positive/negative
                timbre TEXT,                        -- bright/dark
                tonality TEXT,                      -- tonal/atonal
                genre TEXT,                         -- genre
                genre_prob REAL,                    -- probability of genre
                mood TEXT,                          -- mood
                key TEXT                            -- musical key
                )
                       """)
        conn.commit()


def run(db_loc):
    isrc = get_missing_isrc(db_loc)
    mbid = isrc_to_mbid(isrc)
    high, low = extract_data(mbid)
    df = process_data(high, low)
    upload_data(df, db_loc)
