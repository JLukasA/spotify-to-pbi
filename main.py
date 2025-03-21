import spotify_extraction
import acousticbrainz_extraction
import pandas as pd
DATABASE_LOCATION = "sqlite:///my_tracks.sqlite"


if __name__ == "__main__":

    # run spotify extraction and get a pandas dataframe containing uploaded songs and corresponding artists
    song_df = spotify_extraction.run(DATABASE_LOCATION)

    # perform further data extractions
