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
import json
import localserver
from urllib.parse import urlparse

DATABASE_LOCATION = "sqlite:///my_spotify_tracks.sqlite"


def validate_data(df: pd.DataFrame) -> bool:
    """ Quick data validation before uploading to database. """
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
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # dates = df["date"].tolist()
    # for date in dates:
    #    if datetime.datetime.strptime(date, '%Y-%m-%d') != yesterday:
    #        raise Exception("Not all songs are from yesterday")

    return True


def process_data(sp: spotipy.Spotify, tracks, existing_tracks_dict) -> pd.DataFrame:
    """ Process the downloaded spotify data before database upload. Initializes empty lists of all features that will be saved.
        Two loops: first one through all tracks to extract/append available information. Second loop through all artists to add
        corresponding genres to """
    song_name_list, artist_name_list, featured_artist_list = [], [], []
    genre_list, album_name_list = [], []
    duration_list, release_date_list, played_at_list, dates_list = [], [], [], []
    spotify_url_list, track_id_list = [], []
    artist_id_list = []
    missing_ids = []
    # first loop - append available information and create artist_id_list for a second API call
    for idx, song in enumerate(tracks["items"]):
        track = song.get("track", {})
        track_id = track.get("id")
        if not track_id:  # Skip if track_id is missing
            missing_ids.append(idx)
            print(f"NO TRACK ID FOR SONG {song}!!! WARNING WARNING WARNING")
            continue
        track_id_list.append(track_id)
        played_at_list.append(song.get("played_at"))
        dates_list.append(song.get("played_at", "").split("T")[0])
        song_name_list.append(track.get("name"))
        album_name_list.append(track.get("album", {}).get("name"))
        duration_list.append(round(track.get("duration_ms", 0) / 1000))
        release_date_list.append(track.get("album", {}).get("release_date"))
        spotify_url_list.append(track.get("external_urls", {}).get("spotify"))

        # Handle artists
        artists = track.get("artists", [])
        artist_names = [artist.get("name") for artist in artists]
        artist_name_list.append(artist_names[0] if artist_names else "")  # Default to empty string if no artists
        featured_artist_list.append(", ".join(artist_names[1:]) if len(artist_names) > 1 else "")
        artist_id_list.append(artists[0].get("id") if artists else "")  # Default to empty string if no artists

    # API call number two: get artist information
    # store in dict, use id to get information (genre, might extract more information since audio features isn't working.)

    unique_artist_ids = list(set(artist_id_list))
    try:
        artist_info_list = sp.artists(unique_artist_ids)["artists"]
        artist_info_dict = {artist["id"]: artist for artist in artist_info_list}
    except spotipy.exceptions.SpotifyException as e:
        print(f"Error fetching artist information: {e}")
        artist_info_dict = {}

    for id in artist_id_list:
        artist_info = artist_info_dict.get(id)
        genre = ", ".join(artist_info.get("genres", [])) if artist_info else ""
        genre_list.append(genre)

    # Create DataFrame
    data = {
        "played_at": played_at_list,
        "date": dates_list,
        "song_name": song_name_list,
        "main_artist": artist_name_list,
        "featured_artists": featured_artist_list,
        "album_name": album_name_list,
        "genre": genre_list,
        "release_date": release_date_list,
        "duration_sec": duration_list,
        "track_id": track_id_list,
        "artist_id": artist_id_list,
        "spotify_url": spotify_url_list
    }

    df = pd.DataFrame(data)
    return df


def get_database_tracks(s: str):
    """ Fetch track features of tracks already available in the database instead of using API to get already available information. """
    conn = sqlite3.connect(s)
    cursor = conn.cursor()
    cursor.execute("SELECT track_id, genre FROM raw_spotify_data GROUP BY track_id")
    tracks = {row[0]: {
        "genre": row[1]
    }
        for row in cursor.fetchall()
    }
    conn.close()
    return tracks


def initialize_database(s: str):
    """ Initialize database if it doesn't exist. Needed for first run. """

    conn = sqlite3.connect(s)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_spotify_data (       
            played_at TEXT PRIMARY KEY,
            date TEXT,
            song_name TEXT,
            main_artist TEXT,
            featured_artists TEXT,
            album_name TEXT,
            genre TEXT,
            release_date TEXT,
            duration_sec INTEGER,
            track_id TEXT,
            artist_id TEXT,
            spotify_url TEXT
            )
                   """)
    conn.commit()
    conn.close()


if __name__ == "__main__":

    # in terminal, insert client id, client secret, redirect uri for auth manager
    print("To get client id, client secret, and redirect URI visit https://developer.spotify.com/dashboard")
    client_id = input("Enter Client id : ")
    client_secret = getpass("Enter Client Secret : ")
    redirect_uri = input("Enter redirect URI: ")
    scope = "user-read-recently-played user-read-private"

    # generate SpotifyOAuth manager
    auth_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope=scope)

    # get authorize url
    auth_url = auth_manager.get_authorize_url()
    print("Open link: ", auth_url)

    # local server to handle redirect
    parsed_uri = urlparse(redirect_uri)
    server_address = (parsed_uri.hostname, parsed_uri.port)
    localserver.run_server(server_address)

    # get redirect url
    # url_response = input("Paste redirect url response Here :  ")

    # get token, print info
    if "authorization_code" in localserver.__dict__:
        code = localserver.authorization_code
        token_info = auth_manager.get_access_token(code)
        print("Access Token:", token_info["access_token"])
        print("Refresh Token:", token_info["refresh_token"])

        # Initialize Spotify client
        sp = spotipy.Spotify(auth_manager=auth_manager)

        # get UNIX timestamp for yesterday
        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_unix = int(yesterday.timestamp()) * 1000

        # fetch 50 most recently played songs since yesterday
        recently_played_tracks = sp.current_user_recently_played(limit=10, after=yesterday_unix)

        # fetch IDs of songs and artists already in database
        s = DATABASE_LOCATION.replace('sqlite:///', '')
        initialize_database(s)
        existing_tracks = get_database_tracks(s)

        # generate a pandas DataFrame of information for the fetched songs
        # only make further API calls if songs not available in database
        df = process_data(sp, recently_played_tracks, existing_tracks)
        earliest_timestamp, latest_timestamp = df.iloc[0]["played_at"], df.iloc[-1]["played_at"]

        # connect to database
        engine = create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect(s)
        print(f"Connected to database {s}.")
        cursor = conn.cursor()

        # check timestamps of already uploaded data
        cursor.execute("SELECT played_at FROM raw_spotify_data ORDER BY played_at DESC LIMIT 1")
        res = cursor.fetchone()
        latest_uploaded_timestamp = res[0] if res else None

        # if overlaps exist, remove
        if latest_uploaded_timestamp:
            new_data = df[df["played_at"] > latest_uploaded_timestamp]
        else:
            new_data = df

        # sort songs by time played and upload to database
        new_data = new_data.sort_values(by="played_at", ascending=True)
        try:
            new_data.to_sql('raw_spotify_data', engine, index=False, if_exists='append')
            print(f"Data loaded successfully. {len(new_data.index)} songs were uploaded, played between {new_data.iloc[0]["played_at"]} and {new_data.iloc[-1]["played_at"]}.")
        except Exception as e:
            print(f"failed to upload to database {s}. Error : {e}")
        finally:
            conn.close()

    else:
        print("Failed to generate/capture authorization code.")
