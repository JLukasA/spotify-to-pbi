import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import requests
from getpass import getpass
import json
from datetime import datetime
import datetime
import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyOAuth

DATABASE_LOCATION = "sqlite:///my_spotify_tracks.sqlite"


def validate_data(df: pd.DataFrame) -> bool:
    # check if empty
    if df.empty:
        print("DataFrame is empty, no songs were downloaded.")
        return False  # not necessarily error, can have not listened to songs, therefore don't raise exception

    # check for null values
    if df.isnull().values.any():
        raise Exception("DataFrame contains null values")

    # primary key constraint
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key is not unique")

    # check that all songs are from yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("Not all songs are from yesterday")

    return True


def process_data(sp: spotipy.Spotify, tracks, existing_track_ids) -> pd.DataFrame:

    song_name_list, artist_name_list, featured_artist_list = [], [], []
    genre_list, album_name_list = [], []
    duration_list, release_date_list, played_at_list, timestamps = [], [], [], []
    spotify_url_list, track_id_list = [], []

    danceability_list, energy_list, liveness_list, loudness_list = [], [], [], []
    speechiness_list, tempo_list, valence_list = [], [], []

    new_track_ids = []

    for song in (tracks["items"]):
        track = song["track"]
        track_id = track["id"]

        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"].split("T")[0])

        song_name_list.append(track["name"])
        album_name_list.append(track["album"]["name"])
        duration_list.append(round(track["duration_ms"] / 1000))
        release_date_list.append(track["album"]["release_date"])
        spotify_url_list.append(track["external_urls"]["spotify"])
        track_id_list.append(track_id)

        artist_names = [artist["name"] for artist in track["artists"]]
        artist_name_list.append(artist_names[0])
        featured_artist_list.append(", ".join(artist_names[1:]) if len(artist_names) > 1 else "")

        # check if song data is available

        if track_id not in existing_track_ids:
            new_track_ids.append(track_id)
        else:
            genre_list.append(None)
            danceability_list.append(None)
            energy_list.append(None)
            liveness_list.append(None)
            loudness_list.append(None)
            speechiness_list.append(None)
            tempo_list.append(None)
            valence_list.append(None)

    if new_track_ids:

        artist_id_list = [track["artists"][0]["id"] for track in tracks["items"] if track["track"]["id"] in new_track_ids]
        artist_info_list = sp.artists([track["artists"][0]["id"] for track in tracks["items"] if track["track"]["id"] in new_track_ids])["artists"]
        audio_features = sp.audio_features(new_track_ids)

        for idx, track_id in enumerate(new_track_ids):
            artist_info = artist_info_list[idx]
            genre = ", ".join(artist_info.get("genres", []))
            genre_list.append(genre)
            track_features = audio_features[idx]
            if track_features:
                danceability_list.append(track_features.get("danceability"))
                energy_list.append(track_features.get("energy"))
                liveness_list.append(track_features.get("liveness"))
                loudness_list.append(track_features.get("loudness"))
                speechiness_list.append(track_features.get("speechiness"))
                tempo_list.append(track_features.get("tempo"))
                valence_list.append(track_features.get("valence"))

    data = {
        "played_at": played_at_list,
        "timestamp": timestamps,
        "song_name": song_name_list,
        "main_artist": artist_name_list,
        "featured_artists": featured_artist_list,
        "album_name": album_name_list,
        "genre": genre_list,
        "release_date": release_date_list,
        "duration_sec": duration_list,
        "danceability": danceability_list,
        "valence": valence_list,
        "speechiness": speechiness_list,
        "energy": energy_list,
        "tempo": tempo_list,
        "liveness": liveness_list,
        "loudness": loudness_list,
        "track_id": track_id_list,
        "artist_id": artist_id_list,
        "spotify_url": spotify_url_list
    }

    df = pd.DataFrame(data)
    return df


def get_database_track_ids(s: str):
    conn = sqlite3.connect(s)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT track_id FROM raw_spotify_data")
    ids = {row[0] for row in cursor.fetchall()}
    conn.close()
    return ids


if __name__ == "__main__":

    # in terminal, generate client id, secret and URI to
    print("To get client id, client secret, and redirect URI visit https://developer.spotify.com/dashboard")
    c_id = input("Enter Client id : ")
    c_secret = getpass("Enter Client Secret : ")
    r_uri = input("Enter redirect URI.")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=c_id,
                         client_secret=c_secret,
                         redirect_uri=r_uri,
                         scope="user-read-recently-played"))

    # get UNIX timestamp for yesterday
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp()) * 1000

    # fetch played songs since yesterday
    recently_played_tracks = sp.current_user_recently_played(limit=50, after=yesterday_unix)

    # fetch IDs of songs and artists already in database
    s = DATABASE_LOCATION.replace('sqlite:///', '')
    existing_ids = get_database_track_ids(s)

    # generate a pandas DataFrame of information for the fetched songs
    # only make further API calls if songs not available in database
    df = process_data(sp, recently_played_tracks, existing_ids)
    earliest_timetamp, latest_timestamp = df.iloc[0]['timestamp'], df.iloc[-1]['timestamp']

    # connect to database
    engine = create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect(s)
    print(f"Connected to database {s}.")
    cursor = conn.cursor()

    # NOT TIMESTAMP -

    # check timestamps of already uploaded data
    old_df = pd.read_sql(f"SELECT timestamp FROM raw_spotify_data", conn)
    latest_uploaded_timestamp = old_df.iloc[-1]['timestamp']

    # if overlaps exist, remove
    if latest_uploaded_timestamp >= latest_timestamp:
        new_data = df[~df['timestamp'].isin(old_df['timestamp'])]

    # upload songs to database (bronze layer?)
    try:
        new_data.to_sql('raw_spotify_data', engine, index=False, if_exists='append')
    except:
        print(f"failed uptolad data to database {s}.")
