import spotify_extraction
import acousticbrainz_extraction
import sql_operations

DATABASE_LOCATION = "sqlite:///my_tracks.sqlite"

if __name__ == "__main__":

    # run spotify extraction and return a pandas dataframe containing
    spotify_extraction.run(DATABASE_LOCATION)

    # perform further metadata extractions
    acousticbrainz_extraction.run(DATABASE_LOCATION)

    # combine into large table containing all raw data.
    sql_operations.run(DATABASE_LOCATION)

    print("Database has been completely updated.")

    sql_operations.create_brightness_and_danceability_sheet(DATABASE_LOCATION, "./exports")
