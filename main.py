import requests
import sqlite3
import os
from datetime import datetime
from spotify import fetch_spotify_data, get_token
from news import fetch_news_articles

def set_up_database(db_name):
    """
    Sets up the SQLite database and creates the Movies table if it doesn't exist.

    Parameters:
    - db_name (str): Name of the SQLite database file.

    Returns:
    - cursor, connection: Database cursor and connection objects.
    """
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
    api_key = "25781136"  # Replace with your API key
    current_year = datetime.now().year

    cur.execute("SELECT COUNT(*) FROM Movies")
    existing_count = cur.fetchone()[0]
    remaining = limit - existing_count
    
    if remaining <= 0:
        print(f"Database already contains {existing_count} movies. Limit of {limit} reached.")
        return

    movies_count = 0  # Track the number of movies added

    for year in range(start_year, current_year + 1):
        page = 1
        while movies_count < remaining:
            response = requests.get(base_url, params={
                "s": "movie",
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
                                print(f"Skipping movie due to missing data: {full_data}")
                                continue

                            try:
                                cur.execute("""
                                    INSERT OR IGNORE INTO Movies (title, year, genre, country, imdb_rating)
                                    VALUES (?, ?, ?, ?, ?)
                                """, (title, int(year), genre, country, float(imdb_rating)))
                                print(f"Inserting movie: {title} ({year})")
                                movies_count += 1
                                if movies_count >= remaining:
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

    conn.commit()

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

def integrate_spotify(cur, conn, token, movies):
    """
    Fetches Spotify data related to movie titles.

    Parameters:
    - cur: Database cursor.
    - conn: Database connection.
    - token: Spotify API token.
    - movies (list): List of movie tuples.

    Returns:
    - dict: Spotify data related to movies.
    """
    spotify_data = {}
    for movie in movies:
        title = movie[0]
        try:
            spotify_info = fetch_spotify_data(cur, conn, token, title)
            if spotify_info:
                spotify_data[title] = spotify_info
        except Exception as e:
            print(f"Error fetching Spotify data for {title}: {e}")
    return spotify_data

def integrate_news(movies):
    """
    Fetches news articles related to movie titles.

    Parameters:
    - movies (list): List of movie tuples.

    Returns:
    - dict: News articles related to movies.
    """
    news_data = {}
    for movie in movies:
        title = movie[0]
        try:
            news_info = fetch_news_articles(title)
            if news_info:
                news_data[title] = news_info
        except Exception as e:
            print(f"Error fetching news articles for {title}: {e}")
    return news_data

def main():
    """
    Orchestrates the setup, fetching, and querying of movies, and integrates data from Spotify and NewsAPI.
    """
    cur, conn = set_up_database("movies.db")
    fetch_movies_by_year(cur, conn, start_year=2015, limit=100)
    movies = query_movies(cur)
    print(f"\nTotal Movies Fetched: {len(movies)}\n")
    for movie in movies:
        print(movie)

    token = get_token()
    spotify_data = integrate_spotify(cur, conn, token, movies)
    print("\nSpotify Data:")
    for title, data in spotify_data.items():
        print(f"{title}: {data}")

    news_data = integrate_news(movies)
    print("\nNews Articles:")
    for title, articles in news_data.items():
        print(f"{title}: {articles}")

    # Basic data analysis
    print("\nData Analysis:")
    print(f"Total number of movies: {len(movies)}")
    avg_rating = sum(movie[4] for movie in movies) / len(movies)
    print(f"Average IMDb rating: {avg_rating:.2f}")
    
    genre_count = {}
    for movie in movies:
        genres = movie[2].split(', ')
        for genre in genres:
            genre_count[genre] = genre_count.get(genre, 0) + 1
    print("Top 5 genres:")
    for genre, count in sorted(genre_count.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {genre}: {count}")

    conn.close()

if __name__ == "__main__":
    main()