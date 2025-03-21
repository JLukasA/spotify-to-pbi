import spotify_extraction

DATABASE_LOCATION = "sqlite:///my_tracks.sqlite"


if __name__ == "__main__":
    spotify_extraction.run(DATABASE_LOCATION)
