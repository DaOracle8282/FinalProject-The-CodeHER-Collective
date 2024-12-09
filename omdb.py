import requests
import sqlite3
import os
from datetime import datetime

# New function to remove unnecessary database
def remove_database(db_name):
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"Unnecessary database file '{db_name}' has been removed.")
    else:
        print(f"Database file '{db_name}' does not exist.")

# Step 1: Set Up the Database
def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
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

# Function to clear the Movies table
def clear_movies_table(cur, conn):
    cur.execute("DELETE FROM Movies")
    conn.commit()
    print("All movies have been deleted from the database.")

# Step 2: Fetch Movies for 2024
def fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25):
    print(f"Attempting to fetch up to {fetch_limit} movies, with a max total of {max_total}")
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
                imdb_rating = full_data.get("imdbRating", "N/A")
                if imdb_rating == "N/A":
                    imdb_rating = 0.0

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

    print(f"Fetch process completed. Current movie count: {current_count}")

# Step 3: Query Movies from the Database
def query_movies(cur):
    print("Querying movies from the database...")
    cur.execute("""
        SELECT title, year, genre, country, imdb_rating
        FROM Movies
        ORDER BY imdb_rating DESC
    """)
    results = cur.fetchall()
    print(f"Found {len(results)} movies in the database.")
    return results

# Step 4: Main Function
def main():
    print("Starting the movie fetching process...")
    
    # Remove unnecessary database
    remove_database("movies2024.db")
    
    cur, conn = set_up_database("movies.db")
    print("Database connection established.")

    # Fetch movies for 2024, limited to 25 per run, with a maximum of 100 total
    fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25)

    movies = query_movies(cur)
    print(f"\nTotal Movies Fetched: {len(movies)}\n")
    for movie in movies:
        print(movie)

    conn.close()
    print("Database connection closed. Script execution completed.")

# Step 5: Run the Main Function
if __name__ == "__main__":
    main()