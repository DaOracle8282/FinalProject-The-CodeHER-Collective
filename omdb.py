import os
import sqlite3
import requests

def remove_database(db_name):
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"Database '{db_name}' has been removed to start fresh.")
    else:
        print(f"Database '{db_name}' does not exist, no need to remove.")

def set_up_database(db_name):
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

def fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25):
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

                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO Movies (title, year, genre, country, imdb_rating)
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
    db_name = "movies.db"
    remove_database(db_name)
    cur, conn = set_up_database(db_name)
    fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25)
    conn.close()

if __name__ == "__main__":
    main()