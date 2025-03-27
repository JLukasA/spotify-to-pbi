import spotify_extraction
import acousticbrainz_extraction

DATABASE_LOCATION = "sqlite:///my_tracks.sqlite"

if __name__ == "__main__":

    # run spotify extraction and return a pandas dataframe containing
    spotify_extraction.run(DATABASE_LOCATION)

    # perform further metadata extractions
    acousticbrainz_extraction.run(DATABASE_LOCATION)
