
import pandas
import os
import sqlite3
import requests
import matplotlib.pyplot as plt



# Set Up the Database
def set_up_database(db_name):
    """
    Sets up the SQLite database and creates the Movies table.

    Parameters:
    - db_name (str): Name of the SQLite database file.

    Returns:
    - cursor, connection: Database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            year INTEGER,
            genre TEXT,
            director TEXT,
            imdb_rating REAL,
            box_office INTEGER
        )
    """)
    conn.commit()
    return cur, conn



def fetch_movie_data(movie_titles):
    """
    Fetches data from the OMDb API for a list of movie titles.

    Parameters:
    - movie_titles (list): List of movie titles to fetch data for.

    Returns:
    - list: List of movie data dictionaries.
    """
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Your specific API key
    movies_data = []

    for title in movie_titles:
        # Make API request with the provided API key
        response = requests.get(base_url, params={"t": title, "apikey": api_key})
        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                movies_data.append(data)  # Add movie data to the list
            else:
                print(f"Movie not found: {title}")  # Handle missing movies
        else:
            print(f"Error fetching data for {title}: {response.status_code}")  # Handle API errors

    return movies_data




