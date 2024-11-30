import requests
import sqlite3
import os
from datetime import datetime

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    # Drop the table to ensure it has the correct structure
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

def fetch_movies_by_year(cur, conn, start_year=2020, limit=100):
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Replace with your API key
    current_year = datetime.now().year
    movies_count = 0

    for year in range(start_year, current_year + 1):
        page = 1
        while movies_count < limit:
            # Update search to include a general keyword
            response = requests.get(base_url, params={
                "s": "movie",  # Broad search term
                "type": "movie",
                "y": year,
                "page": page,
                "apikey": api_key
            })

            # Debugging: Print the request URL and response
            print(f"Request URL: {response.url}")
            print(f"Response Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                print(f"Response Data: {data}")  # Print response for debugging
                if data.get("Response") == "True":
                    for movie in data.get("Search", []):
                        # Fetch full movie details
                        full_data = requests.get(base_url, params={
                            "i": movie.get("imdbID"),
                            "apikey": api_key
                        }).json()

                        if full_data.get("Response") == "True" and "United States" in full_data.get("Country", ""):
                            # Handle cases where imdbRating is 'N/A'
                            imdb_rating = full_data.get("imdbRating", "N/A")
                            if imdb_rating == "N/A":
                                imdb_rating = 0.0  # Default to 0.0 for missing ratings

                            # Store movie data
                            cur.execute("""
                                INSERT OR IGNORE INTO Movies (title, year, genre, country, imdb_rating)
                                VALUES (?, ?, ?, ?, ?)
                            """, (
                                full_data.get("Title"),
                                int(full_data.get("Year")),
                                full_data.get("Genre"),
                                full_data.get("Country"),
                                float(imdb_rating)
                            ))
                            movies_count += 1
                            if movies_count >= limit:
                                break
                    page += 1
                else:
                    print(f"No more movies found for year: {year}")
                    break
            else:
                print(f"Error fetching movies for year {year}: {response.status_code}")
                break

    conn.commit()

def query_movies(cur):
    cur.execute("""
        SELECT title, year, genre, country, imdb_rating
        FROM Movies
        ORDER BY year DESC, imdb_rating DESC
    """)
    return cur.fetchall()

def main():
    cur, conn = set_up_database("movies.db")
    fetch_movies_by_year(cur, conn, start_year=2020, limit=100)
    movies = query_movies(cur)
    print(f"\nTotal Movies Fetched: {len(movies)}\n")
    for movie in movies:
        print(movie)
    conn.close()

if __name__ == "__main__":
    main()
