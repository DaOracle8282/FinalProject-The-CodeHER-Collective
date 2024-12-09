import requests
import sqlite3
import os
from datetime import datetime

# Step 1: Set Up the Database
def set_up_database(db_name):
    """
    Sets up the SQLite database and creates the Movies table.

    Parameters:
    - db_name (str): Name of the SQLite database file.

    Returns:
    - cursor, connection: Database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    # Create the Movies table if it doesn't exist
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

# New function to clear the Movies table
def clear_movies_table(cur, conn):
    cur.execute("DELETE FROM Movies")
    conn.commit()
    print("All movies have been deleted from the database.")

# Step 2: Fetch Movies by Year
def fetch_movies_by_year(cur, conn, start_year=2015, max_total=25, fetch_limit=25):
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Replace with your API key
    current_year = datetime.now().year

    cur.execute("SELECT COUNT(*) FROM Movies")
    current_count = cur.fetchone()[0]
    print(f"Current count of movies: {current_count}")

    if current_count >= max_total:
        print(f"Database already contains {current_count} movies. Limit of {max_total} reached.")
        return

    remaining = min(max_total - current_count, fetch_limit)  # Number of movies to fetch this time

    for year in range(start_year, current_year + 1):
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
                if remaining <= 0:
                    break

                full_data = requests.get(base_url, params={
                    "i": movie.get("imdbID"),
                    "apikey": api_key
                }).json()

                if full_data.get("Response") == "True" and "United States" in full_data.get("Country", ""):
                    title = full_data.get("Title")
                    year = full_data.get("Year")
                    genre = full_data.get("Genre")
                    country = full_data.get("Country")
                    imdb_rating = full_data.get("imdbRating", "0.0")

                    if not title or not year:
                        continue

                    try:
                        cur.execute("""
                            INSERT OR IGNORE INTO Movies (title, year, genre, country, imdb_rating)
                            SELECT ?, ?, ?, ?, ?
                            WHERE (SELECT COUNT(*) FROM Movies) < ?
                        """, (title, int(year), genre, country, float(imdb_rating), max_total))
                        conn.commit()

                        cur.execute("SELECT COUNT(*) FROM Movies")
                        updated_count = cur.fetchone()[0]

                        if updated_count > current_count:
                            current_count = updated_count
                            remaining -= 1
                            print(f"Inserted: {title} ({year})")

                        if remaining <= 0:
                            break
                    except Exception as e:
                        print(f"Error inserting movie: {title}. Error: {e}")

            page += 1

            if remaining <= 0:
                break

    print(f"Final count of movies in the database: {current_count}")

# Step 3: Query Movies from the Database
def query_movies(cur):
    """
    Queries the Movies table and retrieves all records ordered by year and IMDb rating.

    Parameters:
    - cur: Database cursor.

    Returns:
    - list: List of tuples containing movie data.
    """
    cur.execute("""
        SELECT title, year, genre, country, imdb_rating
        FROM Movies
        ORDER BY year DESC, imdb_rating DESC
    """)
    return cur.fetchall()

# Step 4: Main Function
def main():
    cur, conn = set_up_database("movies.db")
    
    # Clear existing movies
    clear_movies_table(cur, conn)
    
    # Fetch new movies
    fetch_movies_by_year(cur, conn, start_year=2015, max_total=25)
    
    # Query and display the movies
    movies = query_movies(cur)
    print(f"\nTotal Movies Fetched: {len(movies)}\n")
    for movie in movies:
        print(movie)
    
    conn.close()