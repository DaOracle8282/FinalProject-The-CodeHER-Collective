import sqlite3
import requests
import matplotlib.pyplot as plt
import json
import os
import time

# Constants
API_KEY = "3af261f305444139a5defcdd00c4659c"
BASE_URL = "https://newsapi.org/v2/everything"

# Set up the Articles and Sources tables
def setup_articles_table(db_name):
    """
    Sets up the Articles and Sources tables in the existing database.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    try:
        # Recreate the Articles table with published_date as INTEGER
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                article_title TEXT,
                source_id TEXT,
                published_date INTEGER,  -- Changed to INTEGER
                article_content TEXT,
                UNIQUE(movie_id, article_title, published_date),
                FOREIGN KEY (movie_id) REFERENCES Movies(id),
                FOREIGN KEY (source_id) REFERENCES Sources(id)
            )
        """)

        # Sources table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT UNIQUE
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error setting up tables: {e}")
    return conn, cur


# Fetch and store articles from NewsAPI
def fetch_articles(cur, conn, fetch_limit=25):
    """
    Fetches articles related to a specific movie title from NewsAPI and stores them in the database.
    """
    total_articles = 0
    page = 1
    PAGE_SIZE = 25

    headers = {
        'User-Agent': 'NewsAPI-Client/1.0',
        'Accept': 'application/json'
    }

    # Fetch movies from the Movies table
    cur.execute("""
        SELECT id, title 
        FROM Movies 
        WHERE year = 2024
        AND id NOT IN (SELECT movie_id FROM Articles)
    """)
    movies = cur.fetchall()
    if not movies:
        print("No movies from 2024 found in the database.")
        return

    print("Fetching articles for movies from 2024...")

    for movie_id, movie_title in movies:
        while total_articles < fetch_limit:
            print(f"Fetching articles for '{movie_title}', Page: {page}")
            params = {
                'q': movie_title,
                'apiKey': API_KEY,
                'pageSize': PAGE_SIZE,
                'page': page
            }

            try:
                response = requests.get(BASE_URL, params=params, headers=headers)
                if response.status_code == 429:
                    print("Rate limit hit. Sleeping for 60 seconds...")
                    time.sleep(60)
                    continue
                elif response.status_code != 200:
                    print(f"Error fetching articles for '{movie_title}': {response.status_code}")
                    break

                articles = response.json().get("articles", [])
                if not articles:
                    print(f"No more articles found for '{movie_title}'.")
                    break

                for article in articles:
                    article_title = article.get("title", "").strip()
                    source_name = article.get("source", {}).get("name", "").strip()
                    published_date = article.get("publishedAt", "").replace("T", "").replace("Z", "").replace("-", "").replace(":", "")
                    article_content = article.get("content", "").strip()

                    if not published_date.isdigit():
                        continue  # Skip invalid dates

                    # Insert source or retrieve its ID
                    cur.execute("""
                        SELECT id FROM Sources WHERE source_name = ?;
                    """, (source_name,))
                    source_row = cur.fetchone()

                    if source_row:
                        source_id = source_row[0]
                    else:
                        cur.execute("""
                            INSERT INTO Sources (source_name) VALUES (?);
                        """, (source_name,))
                        conn.commit()
                        source_id = cur.lastrowid

                    # Insert into Articles table
                    try:
                        cur.execute("""
                            INSERT OR IGNORE INTO Articles (movie_id, article_title, source_id, published_date, article_content)
                            VALUES (?, ?, ?, ?, ?);
                        """, (movie_id, article_title, source_id, int(published_date), article_content))
                        conn.commit()
                        total_articles += 1
                        print(f"Inserted: {article_title}")
                    except sqlite3.Error as e:
                        print(f"Error inserting article: {e}")

                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Network error: {e}")
                break

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"TOTAL ARTICLES INSERTED INTO TABLE: {total_articles}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


# Main function
def main():
    """
    Main function to orchestrate the process of setting up the database
    and fetching articles.
    """
    conn, cur = setup_articles_table("movies2024.db")
    fetch_articles(cur, conn, fetch_limit=25)
    conn.close()


if __name__ == "__main__":
    main()
