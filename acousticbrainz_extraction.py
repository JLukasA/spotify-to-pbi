import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import requests
import time
from typing import Optional
from urllib.parse import quote
from tqdm import tqdm

with open("musicbrainz_config.txt", "r") as file:
    lines = file.read().splitlines()
    app_name = lines[0]
    email = lines[1]
user_agent = f"{app_name} ({email})"
HEADERS = {
    'User-Agent': user_agent,
    'Accept': 'application/json'
}
AB_API_URL = "https://acousticbrainz.org/api/v1/"


def initialize_databases(engine: Engine) -> None:
    """ Initialize databases if it doesn't exist. """
    with engine.begin() as conn:
        query1 = text("""
            CREATE TABLE IF NOT EXISTS raw_acousticbrainz_data (
                isrc TEXT PRIMARY KEY NOT NULL,     -- International Standard Recording Code
                mbid TEXT UNIQUE,                   -- MusicBrainz ID, UUID format
                danceability TEXT,                  -- danceable/not danceable
                instrumentality TEXT,               -- instrumental/voice
                instrumentality_prob REAL,          -- probability of being instrumental/voice
                gender TEXT,                        -- male/female
                gender_prob REAL,                   -- probability of male/female
                timbre TEXT,                        -- bright/dark
                tonality TEXT                       -- tonal/atonal
                    )
                       """)
        conn.execute(query1)
        query2 = text(""" CREATE TABLE IF NOT EXISTS failed_isrcs(
                     isrc TEXT PRIMARY KEY,                             -- International Standard Recording Code
                     last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- timestamp of fetching attempt 
                     )
                      """)
        conn.execute(query2)
        query3 = text(""" CREATE TABLE IF NOT EXISTS invalid_mbids(
                     mbid TEXT PRIMARY KEY,                             -- Musicbrainz ID
                     isrc TEXT,                                         -- International Standard Recording Code
                     last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- timestamp of fetching attempt 
                     )
                      """)
        conn.execute(query3)


def get_missing_isrc(engine: Engine) -> list[str]:
    """ Returns a list containing ISRC of songs in the spotify table that is neither in the acousticbrainz table, nor has it unsuccessfully been used to fetch MBIDs. """
    with engine.begin() as conn:
        query = text(""" 
                SELECT DISTINCT s.isrc 
                FROM raw_spotify_data s
                LEFT JOIN raw_acousticbrainz_data a on s.isrc = a.isrc
                LEFT JOIN failed_isrcs f on s.isrc = f.isrc
                LEFT JOIN invalid_mbids m on s.isrc = m.isrc
                WHERE s.isrc IS NOT NULL
                AND f.isrc IS NULL
                AND m.isrc IS NULL
                AND a.isrc IS NULL
                """)
        res = conn.execute(query)
        new_isrc = {row.isrc for row in res.fetchall()}
        return list(new_isrc)


def isrc_to_mbid(isrc_list: list[str]) -> tuple[list[Optional[str]], list[str], dict[str, str]]:
    """ Fetch mbid from musicbrainz to use to fetch data from acousticbrainz. Rate limit 300 requests per second. If no mbid available, return isrc in separate list. """
    print(f"starting process of fetching Musicbrainz IDs using ISRC. should take approximately {len(isrc_list)} seconds.")
    mbid_list = []
    failed_conversion_list = []
    mbid_to_isrc = {}
    for isrc in tqdm(isrc_list, desc="parsing ISRCs"):
        url = f"https://musicbrainz.org/ws/2/recording/?query=isrc:{quote(isrc)}&fmt=json"

        while True:
            response = requests.get(url, headers=HEADERS, timeout=10)
            time.sleep(1)
            if response.status_code == 200:
                data = response.json()
                if data.get("recordings"):
                    # print(f"Successfully fetched mbid for ISRC {isrc}")
                    mbid = data["recordings"][0]["id"]
                    mbid_list.append(mbid)
                    mbid_to_isrc[mbid] = isrc
                else:
                    # print(f"No mbid data available for irsc {isrc}.")
                    failed_conversion_list.append(isrc)
                break
            if response.status_code == 429:
                print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                time.sleep(1)
            else:
                print(f"Failed fetching mbid. Status code {response.status_code}.")
                failed_conversion_list.append(isrc)
                break

    print(f"Process finished. For {len(isrc_list)} ISRCs, MBIDs were found for {len(mbid_list)}, and the extraction failed for {len(failed_conversion_list)}.")
    return mbid_list, failed_conversion_list, mbid_to_isrc


def extract_data(mbid_list: list[str]) -> tuple[dict[str, dict], list[str]]:
    """ Extract high-level data about Spotify tracks using the acousticbrainz API. Rate limit 10 requests per 10 seconds. """
    print("Acousticbrainz data extraction initiated.")
    ab_data = {}
    invalid_mbids = []
    for mbid in mbid_list:
        if not mbid:
            print("MBID is None, skipping.")
            continue
        # extract high-level data
        url = f"{AB_API_URL}{mbid}/high-level"

        while True:
            res = requests.get(url, headers=HEADERS, timeout=10)

            if res.status_code == 200:
                # print(f"Success fetching high-level data for mbid {mbid}.")
                ab_data[mbid] = res.json()
                break
            elif res.status_code == 429:
                # print(f"Rate limit exceeded. Pausing until extraction can be resumed.")
                time.sleep(10)
            elif res.status_code == 404:
                # print(f"No acoustic data found at {url_high}.")
                invalid_mbids.append(mbid)
                break
            else:
                # print(f"Failed fetching high-level data. Status code {res_high.status_code}")
                break

    invalid_mbids = list(set(invalid_mbids))
    print(
        f"Acousticbrainz data extraction finished. Out of {len(mbid_list)} MBIDs, data was found for {len(ab_data)}. {len(invalid_mbids)} invalid MBIDs.")
    return ab_data, invalid_mbids


def process_data(input_data: dict[str, dict], mbid_list: list[str], invalid_mbids: list[str], mbid_to_isrc: dict[str, str]) -> pd.DataFrame:

    output_data = []
    for mbid in mbid_list:
        if not mbid:
            print(f"no MBID for ISRC")
            continue
        if mbid in invalid_mbids:
            continue
        isrc = mbid_to_isrc.get(mbid)
        if not isrc:
            print(f"Error with fetching isrc using MBID {mbid}.")
            continue

        high = input_data.get(mbid, {}).get("highlevel", {})
        features = {
            "isrc": isrc,
            "mbid": mbid,
            "danceability": high.get("danceability", {}).get("value"),
            "instrumentality": high.get("voice_instrumental", {}).get("value"),
            "instrumentality_prob": high.get("voice_instrumental", {}).get("probability"),
            "gender": high.get("gender", {}).get("value"),
            "gender_prob": high.get("gender", {}).get("probability"),
            "timbre": high.get("timbre", {}).get("value"),
            "tonality": high.get("tonal_atonal", {}).get("value")
        }

        output_data.append(features)

    df = pd.DataFrame(output_data)
    return df


def upload_data(acousticbrainz_df: pd.DataFrame, failed_isrc: list[str], invalid_mbids: list[str], mbid_to_isrc: dict[str, str], engine: Engine) -> None:
    """ Uploads acousticbrainz data and obsolete ISRC to the local database. """
    with engine.begin() as conn:

        if not acousticbrainz_df.empty:
            query = text(""" SELECT mbid FROM raw_acousticbrainz_data""")
            res = conn.execute(query)
            uploaded_mbids = {row.mbid for row in res}
            #   filter
            new_df = acousticbrainz_df[~acousticbrainz_df['mbid'].isin(uploaded_mbids)]
            if not new_df.empty:
                new_df.to_sql('raw_acousticbrainz_data', con=conn, index=False, if_exists='append')
                print(f"Uploaded acoustricbrainz metadata for {len(acousticbrainz_df.index)} songs.")
            else:
                print("No songs to upload.")

        if failed_isrc:
            failed_isrc_df = pd.DataFrame({
                'isrc': failed_isrc,
                'last_attempt': pd.Timestamp.utcnow()
            })
            failed_isrc_df.to_sql('failed_isrcs', con=conn, index=False, if_exists='append')
            print(f"Logged {len(failed_isrc)} Failed ISRCs.")

        if invalid_mbids:
            invalid_mbid_data = []
            for mbid in invalid_mbids:
                if mbid in mbid_to_isrc:
                    isrc = mbid_to_isrc.get(mbid)
                    invalid_mbid_data.append({
                        'mbid': mbid,
                        'isrc': isrc,
                        'last_attempt': pd.Timestamp.utcnow()
                    })
            if invalid_mbid_data:
                invalid_mbid_df = pd.DataFrame(invalid_mbid_data)
                invalid_mbid_df.to_sql('invalid_mbids', con=conn, index=False, if_exists='append')
                print(f"logged {len(invalid_mbid_data)} failed MBIDs.")


def run(db_loc) -> None:
    engine = create_engine(db_loc)
    try:
        initialize_databases(engine)
        isrc = get_missing_isrc(engine)
        if not isrc:
            print("No new records to add to database. skipping")
            return
        mbid, failed_isrc, mbid_to_isrc = isrc_to_mbid(isrc)
        data, invalid_mbids = extract_data(mbid)
        df = process_data(data, mbid, invalid_mbids, mbid_to_isrc)
        upload_data(df, failed_isrc, invalid_mbids, mbid_to_isrc, engine)
    except Exception as e:
        print(f"Pipeline failure : {e}.")
    finally:
        engine.dispose()
