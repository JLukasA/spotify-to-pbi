import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import datetime
import sqlite3
import requests
import time
from typing import Optional

AB_API_URL = "https://acousticbrainz.org/api/v1/"


def initialize_databases(engine):
    """ Initialize databases if it doesn't exist. Needed for first run. """
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
        conn.execute(""" CREATE TABLE IF NOT EXISTS failed_isrcs(
                     isrc TEXT PRIMARY KEY,                             -- International Standard Recording Code
                     last_attempt TIMESTAMP DEFAULT CURRENT TIMESTAMP   -- timestamp of fetching attempt 
                     )
                      """)
        conn.commit()
    engine.dispose()


def get_missing_isrc(engine) -> list[str]:
    """ Returns a list containing ISRC of songs in the spotify table that is neither in the acousticbrainz table, nor has it unsuccessfully been used to fetch MBIDs. """
    with engine.connect() as conn:
        res = conn.execute(""" 
                              SELECT s.isrc 
                              FROM raw_spotify_data s
                              LEFT JOIN raw_acousticbrainz_data a on s.isrc = a.isrc
                              LEFT JOIN failed_isrc_mappings f on s.isrc = f.isrc
                              WHERE a.isrc IS NULL
                              AND f.isrc IS NULL
                              and s.isrc IS NOT NULL
                              GROUP BY s.isrc
                              """)
        new_isrc = {row.isrc for row in res.fetchall()}
        return list(new_isrc)


def isrc_to_mbid(isrc_list: list[str]) -> tuple[list[Optional[str]], list[str]]:
    """ Fetch mbid from musicbrainz to use to fetch data from acousticbrainz. Rate limit 300 requests per second. If no mbid available, return isrc in separate list. """
    mbid_list = []
    failed_conversion_list = []
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
                    failed_conversion_list.append(isrc)
                    mbid_list.append(None)
                break
            if res.status_code == 429:
                print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                time.sleep(1)
            else:
                print(f"Failed fetching mbid. Status code {res.status_code}.")
                failed_conversion_list.append(isrc)
                mbid_list.append(None)
                break

    return mbid_list, failed_conversion_list


def extract_data(mbid_list: list[str]) -> tuple[dict[str, dict], dict[str, dict]]:
    """ Extract high- and low-level metadata about Spotify tracks using the acousticbrainz API. Rate limit 10 requests per 10 seconds. """
    ab_data_high = {}
    ab_data_low = {}
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


def process_data(high_level_data: dict[str, dict], low_level_data: dict[str, dict], isrc_list: list[str], mbid_list: list[str]) -> pd.DataFrame:

    data = []
    for isrc, mbid in zip(isrc_list, mbid_list):
        if not mbid:
            print(f"no MBID for ISRC {isrc}")
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


def upload_data(acousticbrainz_df: pd.DataFrame, failed_isrc: list[str], engine):
    """ Uploads acousticbrainz data and obsolete ISRC to the local database. """

    try:
        with engine.begin() as conn:
            if not acousticbrainz_df.empty:
                acousticbrainz_df.to_sql('raw_acousticbrainz_data', con=conn, index=False, if_exists='append')
                print(f"Uploaded acoustricbrainz metadata for {len(acousticbrainz_df.index)} songs.")
            if failed_isrc:
                failed_isrc_df = pd.DataFrame({
                    'isrc': failed_isrc,
                    'last_attempt': pd.Timestamp.utcnow()
                })
                failed_isrc_df.to_sql('failed_isrc', con=conn, index=False, if_exists='append')
                print(f"Uploaded {len(failed_isrc)} ISRC's of songs with no corresponding MBID.")
    except Exception as e:
        print(f"failed to upload to database. Error : {e}")
        raise


def run(db_loc):
    engine = create_engine(db_loc)
    try:
        initialize_databases(engine)
        isrc = get_missing_isrc(engine)
        mbid, failed_isrc = isrc_to_mbid(isrc)
        high, low = extract_data(mbid)
        df = process_data(high, low)
        upload_data(df, failed_isrc, engine)
    except Exception as e:
        print(f"Pipeline failure : {e}.")
    finally:
        engine.dispose()
