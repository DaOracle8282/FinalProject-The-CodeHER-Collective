
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


# Fetch Movie Data from OMDb API
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


# Store Data into the Database
def store_movie_data(movies_data, cur, conn):
    """
    Stores up to 25 movie records in the database, avoiding duplicates.

    Parameters:
    - movies_data (list): List of movie data dictionaries.
    - cur: SQLite database cursor.
    - conn: SQLite database connection.

    Returns:
    - None
    """
    count = 0
    for movie in movies_data:
        if count >= 25:  # Limit to 25 items per run
            break

        try:
            cur.execute("""
                INSERT OR IGNORE INTO Movies (title, year, genre, director, imdb_rating, box_office)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                movie.get("Title", "N/A"),
                int(movie.get("Year", "0")),
                movie.get("Genre", "N/A"),
                movie.get("Director", "N/A"),
                float(movie.get("imdbRating", "0.0")),
                int(movie.get("BoxOffice", "0").replace("$", "").replace(",", "") or 0)
            ))
            count += 1
        except sqlite3.IntegrityError:
            print(f"Duplicate entry skipped: {movie.get('Title')}")

    conn.commit()

