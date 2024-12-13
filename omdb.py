import os
import sqlite3
import requests
import re  # For sanitizing table names

def set_up_database(db_name):
    """
    Sets up the SQLite database for general configuration.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # Create lookup tables for Places (Countries) and Genres
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Places (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            place TEXT UNIQUE
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre TEXT UNIQUE
        )
    """)
    
    # Create main Movies table with foreign keys to Places and Genres
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            year INTEGER,
            imdb_rating REAL,
            place_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY(place_id) REFERENCES Places(id),
            FOREIGN KEY(genre_id) REFERENCES Genres(id)
        )
    """)
    
    return cur, conn

def get_or_create_lookup_id(cur, table, column, value):
    """
    Gets the id for a value in a lookup table or inserts it if it doesn't exist.
    """
    cur.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))
        return cur.lastrowid

def fetch_movies_2024(cur, conn, max_insert=25):
    """
    Fetches up to `max_insert` movies from the year 2024 using the OMDB API
    and populates the database.
    """
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Replace with your valid API key
    year = 2024
    current_insert_count = 0
    page = 1

    print(f"Starting fetch to add up to {max_insert} movies from year {year}.")

    while current_insert_count < max_insert:
        response = requests.get(base_url, params={
            "s": "movie",
            "type": "movie",
            "y": year,
            "page": page,
            "apikey": api_key
        })

        if response.status_code != 200:
            print(f"Error fetching movies: {response.status_code}")
            return

        data = response.json()
        if data.get("Response") != "True":
            print(f"No more movies found on page {page}.")
            return

        for movie in data.get("Search", []):
            if current_insert_count >= max_insert:
                print("Stopping fetch: Max insert limit reached.")
                break

            # Fetch full movie details
            imdb_id = movie.get("imdbID")
            full_data = requests.get(base_url, params={"i": imdb_id, "apikey": api_key}).json()

            if full_data.get("Response") == "True":
                title = full_data.get("Title")
                country = full_data.get("Country", "Unknown").split(",")[0].strip()
                genre = full_data.get("Genre", "N/A").split(",")[0].strip()
                imdb_rating = full_data.get("imdbRating", "0.0")

                if imdb_rating == "N/A":
                    imdb_rating = 0.0

                # Lookup or create IDs for place and genre
                place_id = get_or_create_lookup_id(cur, "Places", "place", country)
                genre_id = get_or_create_lookup_id(cur, "Genres", "genre", genre)

                # Insert into Movies table
                try:
                    cur.execute("""
                        INSERT INTO Movies (title, year, imdb_rating, place_id, genre_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, (title, year, float(imdb_rating), place_id, genre_id))
                    conn.commit()
                    print(f"Inserted into Movies: {title}")
                    current_insert_count += 1
                except sqlite3.IntegrityError:
                    print(f"Duplicate detected: {title}")

        page += 1

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"Fetch process completed. Movies added this run: {current_insert_count}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

def main():
    """
    Main function for setting up the database and fetching movies.
    """
    db_name = "movies2024.db"  # Updated database name
    cur, conn = set_up_database(db_name)

    # Fetch movies and populate database tables
    fetch_movies_2024(cur, conn, max_insert=25)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
