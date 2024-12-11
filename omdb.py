import os
import sqlite3
import requests

def set_up_database(db_name):
    """
    Sets up the SQLite database and creates the Movies table if it doesn't exist.
    
    Arguments:
    db_name (str): The name of the database file.
    
    Returns:
    tuple: A cursor and connection object for the database.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            year INTEGER,
            genre TEXT,
            country TEXT,
            imdb_rating REAL
        )
    """)
    conn.commit()
    return cur, conn

def movie_exists(cur, title):
    """
    Checks if a movie with the given title already exists in the database.

    Arguments:
    cur (sqlite3.Cursor): The database cursor.
    title (str): The title of the movie to check.

    Returns:
    bool: True if the movie exists, False otherwise.
    """
    cur.execute("SELECT 1 FROM Movies WHERE title = ?", (title,))
    return cur.fetchone() is not None

def fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25):
    """
    Fetches movies from the year 2024 using the OMDB API and stores them in the database.
    
    Arguments:
    cur (sqlite3.Cursor): The database cursor.
    conn (sqlite3.Connection): The database connection.
    max_total (int): The maximum total number of movies to store in the database.
    fetch_limit (int): The maximum number of movies to fetch in a single run.
    """
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Replace with your API key
    year = 2024

    cur.execute("SELECT COUNT(*) FROM Movies")
    current_count = cur.fetchone()[0]
    print(f"Current count of movies: {current_count}")

    if current_count >= max_total:
        print(f"Database already contains {current_count} movies. Limit of {max_total} reached.")
        return

    remaining = min(max_total - current_count, fetch_limit)
    page = 1

    while remaining > 0:
        response = requests.get(base_url, params={
            "s": "movie",
            "type": "movie",
            "y": year,
            "page": page,
            "apikey": api_key
        })

        if response.status_code != 200:
            print(f"Error fetching movies for year {year}, page {page}: {response.status_code}")
            return

        data = response.json()
        if data.get("Response") != "True":
            print(f"No more movies found for year {year}, page {page}.")
            break

        for movie in data.get("Search", []):
            full_data = requests.get(base_url, params={
                "i": movie.get("imdbID"),
                "apikey": api_key
            }).json()

            if full_data.get("Response") == "True" and "United States" in full_data.get("Country", ""):
                title = full_data.get("Title")
                year = full_data.get("Year")
                genre = full_data.get("Genre")
                country = full_data.get("Country")
                imdb_rating = full_data.get("imdbRating", "N/A")
                if imdb_rating == "N/A":
                    imdb_rating = 0.0

                if not title or not year:
                    continue

                # Check for duplicates in the database
                if movie_exists(cur, title):
                    print(f"Skipping duplicate movie: {title}")
                    continue

                try:
                    cur.execute("""
                        INSERT INTO Movies (title, year, genre, country, imdb_rating)
                        VALUES (?, ?, ?, ?, ?)
                    """, (title, int(year), genre, country, float(imdb_rating)))
                    conn.commit()

                    if cur.rowcount > 0:
                        remaining -= 1
                        print(f"Inserted: {title} ({year})")

                    if remaining <= 0:
                        break
                except Exception as e:
                    print(f"Error inserting movie: {title}. Error: {e}")

        page += 1

    cur.execute("SELECT COUNT(*) FROM Movies")
    final_count = cur.fetchone()[0]
    print(f"Fetch process completed. Current movie count: {final_count}")

def main():
    """
    The main function that orchestrates the database setup and movie fetching process.
    """
    db_name = "moviestester1.db"
    cur, conn = set_up_database(db_name)
    fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25)
    conn.close()

if __name__ == "__main__":
    main()
