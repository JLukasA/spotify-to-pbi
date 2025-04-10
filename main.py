import spotify_extraction
import acousticbrainz_extraction
import sql_operations

DATABASE_LOCATION = "sqlite:///my_tracks.sqlite"

if __name__ == "__main__":

    while True:
        ans = input("Do you want to update the database with new data? Answer with Yes/y or No/n: ").upper()

        if ans in ["YES", "Y", "NO", "N"]:
            break
        else:
            print("Invalid input. Answer with yes/y or no/n.")

    if ans in ["YES", "Y"]:
        # run spotify extraction
        spotify_extraction.run(DATABASE_LOCATION)
        # perform further metadata extractions
        acousticbrainz_extraction.run(DATABASE_LOCATION)
        # combine into large table containing all raw data.
        sql_operations.run(DATABASE_LOCATION)

    while True:
        ans2 = input("Do you want to create excel-tables using the data available in the database? Answer with Yes/y or No/n: ").upper()

        if ans2 in ["YES", "Y", "NO", "N"]:
            break
        else:
            print("Invalid input. Answer with yes/y or no/n.")

    if ans2 in ["YES", "Y"]:
        sql_operations.create_brightness_and_danceability_hourly_sheet(DATABASE_LOCATION, "./exports")
        sql_operations.create_spotify_hourly_sheet(DATABASE_LOCATION, "./exports")
