import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd
import datetime
from datetime import datetime
import os


def initialise_large_table(engine: Engine) -> None:
    with engine.begin() as conn:
        query = text(("""
            CREATE TABLE IF NOT EXISTS raw_data (
                played_at TEXT PRIMARY KEY,
                date TEXT,
                song_name TEXT,
                main_artist TEXT,
                featured_artists TEXT,
                album_name TEXT,
                artist_genre TEXT,
                release_date TEXT,
                duration_sec INTEGER,
                track_id TEXT,
                artist_id TEXT,
                spotify_url TEXT,
                isrc TEXT,
                mbid TEXT UNIQUE,
                danceability TEXT,
                instrumentality TEXT,
                instrumentality_prob REAL,
                gender TEXT,
                gender_prob REAL,
                timbre TEXT,
                tonality TEXT
            )
        """))
        conn.execute(query)


def update_large_table(engine: Engine) -> None:
    with engine.begin() as conn:

        query1 = text(""" SELECT MAX(played_at) FROM raw_data """)
        latest = conn.execute(query1).scalar()

        query2 = text(""" INSERT OR IGNORE INTO raw_data
                SELECT
                s.played_at, s.date, s.song_name, s.main_artist,
                s.featured_artists, s.album_name, s.artist_genre,
                s.release_date, s.duration_sec, s.track_id, s.artist_id,
                s.spotify_url, s.isrc,
                a.mbid, a.danceability,a.instrumentality, a.instrumentality_prob,
                a.gender, a.gender_prob,  a.timbre, a.tonality
                FROM raw_spotify_data s
                LEFT JOIN raw_acousticbrainz_data a ON s.isrc = a.isrc
                WHERE :latest IS NULL or s.played_at > :latest """)
        conn.execute(query2, {"latest": latest})

        query3 = text(""" SELECT changes() """)
        insertion_count = conn.execute(query3).scalar()
        print(f"Added {insertion_count} new rows to table raw_data.")


def template_db_query(db_loc: str, output_directory: str = "./exports") -> str:
    os.makedirs(output_directory, exist_ok=True)
    engine = create_engine(db_loc)
    with engine.begin() as conn:

        # get data
        query = text("""
        SELECT
        FROM raw_data
        WHERE
        ORDER BY
    """)
        df = pd.read_sql(query, conn)

        # transform
        # df[''] = df[''].round(2)

        # export to Excel
        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = ""  # "features_by_day","features_by_hour" "genre_analysis"
        output_path = f"{output_directory}/{table_name}_{current_timestamp}.xlsx"
        df.to_excel(output_path)

    engine.dispose()


def create_hourly_sheet(db_loc: str, output_directory: str = "./exports") -> None:
    os.makedirs(output_directory, exist_ok=True)
    engine = create_engine(db_loc)
    with engine.begin() as conn:
        query_hourly = text("""
        SELECT
            CAST(strftime('%H', datetime(played_at)) AS INTEGER) AS hour_of_day,
            ROUND(AVG(CASE
                        WHEN danceability = 'danceable' THEN 1
                        WHEN danceability = 'not_danceable' THEN 0
                        ELSE 0.5
                    END), 2) AS danceability_score,
            ROUND(AVG(CASE
                        WHEN timbre = 'bright' THEN 1
                        WHEN timbre = 'dark' then 0
                        ELSE 0.5
                    END), 2) AS brightness_score,
            ROUND(AVG(CASE
                        WHEN gender = 'male' THEN gender_prob
                        WHEN gender = 'female' then -gender_prob
                        ELSE 0
                    END), 2) AS male_score,
            COUNT(*) as entries
        FROM raw_data
        WHERE danceability IS NOT NULL
        AND timbre IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """)

        df_hourly = pd.read_sql(query_hourly, conn)
        query_genres = text("""
        SELECT played_at, artist_genre
        FROM raw_data
        WHERE artist_genre != ''
        """)
        df_genres = pd.read_sql(query_genres, conn)
        df_genres['artist_genre'] = df_genres['artist_genre'].str.split(',\\s*')
        exploded_df = df_genres.explode('artist_genre')
        exploded_df['hour'] = pd.to_datetime(exploded_df['played_at']).dt.hour
        genre_counts = exploded_df.groupby(['hour', 'artist_genre']).size().reset_index(name='count')
        most_common_genres = genre_counts.loc[genre_counts.groupby('hour')['count'].idxmax()]
        most_common_genres = most_common_genres.rename(columns={'artist_genre': 'most_common_genre'})

        df_final = df_hourly.merge(
            most_common_genres[['hour', 'most_common_genre']],
            left_on='hour_of_day',
            right_on='hour',
            how='left'
        )
        df_final = df_final[['hour_of_day', 'brightness_score', 'danceability_score', 'most_common_genre', 'male_score', 'entries']]
        df_final['hour_of_day'] = df_final['hour_of_day'] + 2 % 24  # from ISO8601 to CET
        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = "data_by_hour"
        output_path = f"{output_directory}/{table_name}_{current_timestamp}.xlsx"
        df_final.to_excel(output_path, index=False)


def create_large_sheet(db_loc: str, output_directory: str = "./exports") -> None:
    os.makedirs(output_directory, exist_ok=True)
    engine = create_engine(db_loc)
    with engine.begin() as conn:

        # get data
        query = text("""
        SELECT *
        FROM raw_data
        ORDER BY played_at ASC
        """)
        df = pd.read_sql(query, conn)
        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = "large_sheet"  # "features_by_day","features_by_hour" "genre_analysis"
        output_path = f"{output_directory}/{table_name}_{current_timestamp}.xlsx"
        df.to_excel(output_path)

    engine.dispose()


def run(db_loc):
    engine = create_engine(db_loc)
    try:
        initialise_large_table(engine)
        update_large_table(engine)

    finally:
        engine.dispose()
