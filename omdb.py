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
    # Drop the table to ensure it's clean for each run
    cur.execute("DROP TABLE IF EXISTS Movies")
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

# Step 2: Fetch Movies by Year
def fetch_movies_by_year(cur, conn, start_year=2015, limit=100):
    """
    Fetches movies from the OMDb API based on year and inserts them into the database.

    Parameters:
    - cur: Database cursor.
    - conn: Database connection.
    - start_year (int): Starting year for fetching movies.
    - limit (int): Maximum number of movies to fetch.

    Returns:
    - None
    """
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Replace with API key
    current_year = datetime.now().year
    movies_count = 0  # Track the number of movies added

    for year in range(start_year, current_year + 1):
        page = 1
        while movies_count < limit:
            response = requests.get(base_url, params={
                "s": "movie",  # Broad search term
                "type": "movie",
                "y": year,
                "page": page,
                "apikey": api_key
            })
            print(f"Fetching year: {year}, page: {page}")

            if response.status_code == 200:
                data = response.json()
                if data.get("Response") == "True":
                    for movie in data.get("Search", []):
                        # Fetch full movie details
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
                                imdb_rating = 0.0  # Default missing ratings to 0.0

                            # Ensure critical fields are not missing
                            if not title or not year:
                                print(f"Skipping movie due to missing data: {full_data}")
                                continue

                            try:
                                # Insert movie data into the database
                                cur.execute("""
                                    INSERT OR IGNORE INTO Movies (title, year, genre, country, imdb_rating)
                                    VALUES (?, ?, ?, ?, ?)
                                """, (title, int(year), genre, country, float(imdb_rating)))
                                print(f"Inserting movie: {title} ({year})")
                                movies_count += 1
                                if movies_count >= limit:
                                    break
                            except Exception as e:
                                print(f"Error inserting movie: {title}. Error: {e}")
                    page += 1
                else:
                    print(f"No more movies found for year: {year}")
                    break
            else:
                print(f"Error fetching movies for year {year}: {response.status_code}")
                break

    conn.commit()  # Save changes to the database

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
    """
    Orchestrates the setup, fetching, and querying of movies.
    """
    # Step 4.1: Set up the database
    cur, conn = set_up_database("movies.db")

    # Step 4.2: Fetch movies starting from 2015 and limited to 100
    fetch_movies_by_year(cur, conn, start_year=2015, limit=100)

    # Step 4.3: Query and display the movies
    movies = query_movies(cur)
    print(f"\nTotal Movies Fetched: {len(movies)}\n")
    for movie in movies:
        print(movie)

    # Step 4.4: Close the database connection
    conn.close()

# Step 5: Run the Main Function
if __name__ == "__main__":
    main()
