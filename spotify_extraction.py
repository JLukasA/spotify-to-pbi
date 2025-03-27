import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import Engine
from getpass import getpass
from datetime import datetime
import datetime
import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import localserver
from urllib.parse import urlparse


def establish_spotify_connection() -> spotipy.Spotify:
    """ Establish connection to Spotify. Uses client id and secret to generate token from local server. """

    with open("spotify_config.txt", "r") as file:
        lines = file.read().splitlines()
        client_id = lines[0]
        client_secret = lines[1]
        redirect_uri = lines[2]

    # generate SpotifyOAuth manager
    auth_manager = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope="user-read-recently-played")

    # get authorize url
    auth_url = auth_manager.get_authorize_url()
    print("Open link: ", auth_url)

    # local server to handle redirect
    parsed_uri = urlparse(redirect_uri)
    server_address = (parsed_uri.hostname, parsed_uri.port)
    localserver.run_server(server_address)

    if "authorization_code" in localserver.__dict__:
        code = localserver.authorization_code
        token_info = auth_manager.get_access_token(code)
        print("Access Token:", token_info["access_token"])
        print("Refresh Token:", token_info["refresh_token"])

        # Initialize Spotify client
        sp = spotipy.Spotify(auth_manager=auth_manager)
        return sp
    else:
        print("Failed to generate/capture authorization code.")


def extract_spotify_data(sp: spotipy.Spotify) -> dict:
    """ Fetches information about recently played tracks on Spotify. """

    today = datetime.datetime.now(datetime.timezone.utc)
    yesterday_unix = int((today - datetime.timedelta(days=1)).timestamp() * 1000)

    # fetch up to 50 recently played songs
    tracks = sp.current_user_recently_played(limit=50, after=yesterday_unix)

    # print("tracks type:", tracks.__class__, flush=True)

    return tracks


def process_data(sp: spotipy.Spotify, tracks) -> pd.DataFrame:
    """ Process the downloaded spotify data before database upload. Initializes empty lists of all features that will be saved.
        Two loops: first one through all tracks to extract/append available information. Second loop through all artists to add
        corresponding genres to the dataframe. """
    song_name_list, artist_name_list, featured_artist_list = [], [], []
    genre_list, album_name_list = [], []
    duration_list, release_date_list, played_at_list, dates_list = [], [], [], []
    spotify_url_list, track_id_list, isrc_list = [], [], []
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
        isrc_list.append(track.get("external_ids", {}).get("isrc"))

        # Handle artists
        artists = track.get("artists", [])
        artist_names = [artist.get("name") for artist in artists]
        artist_name_list.append(artist_names[0] if artist_names else "")  # Default to empty string if no artists
        featured_artist_list.append(", ".join(artist_names[1:]) if len(artist_names) > 1 else "")
        artist_id_list.append(artists[0].get("id") if artists else "")  # Default to empty string if no artists

    # API call number two: get artist information
    # store in dict, use id to get information (genre, might extract more information since audio features isn't working.)

    unique_artist_ids = [id for id in set(artist_id_list) if id]
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
        "spotify_url": spotify_url_list,
        "isrc": isrc_list
    }

    df = pd.DataFrame(data)
    return df


def get_database_tracks(engine) -> dict:
    """ Fetch track features of tracks already available in the database instead of using API to get already available information. """
    tracks = {}
    try:
        with engine.connect() as conn:
            res = conn.execute("SELECT track_id, genre FROM raw_spotify_data GROUP BY track_id")
            tracks = {row.track_id: {"genre": row.genre} for row in res}
            return tracks
    except exc.SQLAlchemyError as e:
        print(f"Database error: {e}.")


def initialize_database(engine):
    """ Initialize database if it doesn't exist. Needed for first run. """
    with engine.connect() as conn:
        conn.execute("""
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
                spotify_url TEXT,
                isrc TEXT
                )
                       """)
        conn.commit()


def upload_data(df: pd.DataFrame, db_loc):
    """ Establishes a connection to and uploads the DataFrame to the local SQLite database."""
    if df.empty:
        print("DataFrame is empty, no data to upload.")
        return

    # Establish connection to database and initialize table if it doesn't exist
    s = db_loc.replace('sqlite:///', '')
    engine = create_engine(db_loc)
    initialize_database(engine)
    print(f"Connected to database {s}.")

    # check timestamps of already uploaded data
    with engine.connect() as conn:
        res = conn.execute("SELECT played_at FROM raw_spotify_data ORDER BY played_at DESC LIMIT 1")
        timestamp = res.fetchone()
        latest_uploaded_timestamp = timestamp[0] if timestamp else None

    earliest_timestamp, latest_timestamp = df.iloc[-1]["played_at"], df.iloc[0]["played_at"]

    # if overlaps exist, remove
    if latest_uploaded_timestamp and latest_uploaded_timestamp > latest_timestamp:
        print(f"Overlap in timestamps, latest uploaded song played at {latest_uploaded_timestamp} and earliest downloaded song played at {earliest_timestamp}.")
        new_data = df[df["played_at"] > latest_uploaded_timestamp]
        print(f"DataFrame filtered. Out of {len(df.index)}, {len(new_data.index)} songs were played after {latest_uploaded_timestamp} and will be uploaded to the database.")
    else:
        new_data = df

    # sort songs by time played, validate, and try to upload to database, then close connetion
    new_data = new_data.sort_values(by="played_at", ascending=True)

    if not validate_data(new_data):
        print(f"Data did not pass validation.")
        return

    try:
        new_data.to_sql('raw_spotify_data', engine, index=False, if_exists='append')
        print(f"Data loaded successfully. {len(new_data.index)} songs were uploaded, played between {new_data.iloc[0]["played_at"]} and {new_data.iloc[-1]["played_at"]}.")
    except Exception as e:
        print(f"failed to upload to database {s}. Error : {e}")
    finally:
        engine.dispose()


def validate_data(df: pd.DataFrame) -> bool:
    """ Quick data validation before uploading to database. """
    # check if empty
    if df.empty:
        print("DataFrame is empty, no songs were downloaded.")
        return False  # not necessarily error, can have not listened to songs, therefore don't raise exception

    # check for null values
    # if df.isnull().values.any():
    #    raise Exception("DataFrame contains null values")

    # primary key constraint
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key is not unique")

    return True


def run(db_loc):
    """ Runs the Spotify data extraction. Establishes a connection to the Spotify API, fetches information about recently played songs, loads it into a pandas DataFrame
      and uploads that DataFrame to a local SQLite database. Returns a dataframe containing a list of the uploaded songs/artists."""
    sp = establish_spotify_connection()
    recently_played_tracks = extract_spotify_data(sp)
    df = process_data(sp, recently_played_tracks)
    upload_data(df, db_loc)
