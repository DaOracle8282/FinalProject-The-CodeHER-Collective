import sqlite3
import requests
import matplotlib.pyplot as plt
import json
import os
import time
from datetime import datetime

# Constants
API_KEY = "a87807265caf4776a826977908066096"  # Updated API key
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                article_title TEXT,
                source_id INTEGER,
                published_date REAL,  -- Converted to REAL (epoch time)
                article_content TEXT,
                UNIQUE(movie_id, article_title, published_date),
                FOREIGN KEY (movie_id) REFERENCES Movies(id),
                FOREIGN KEY (source_id) REFERENCES Sources(id)
            )
        """)

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

# Convert ISO 8601 datetime string to Unix epoch time
def convert_to_unix_epoch(published_date):
    """
    Converts ISO 8601 date string to Unix epoch time.
    """
    try:
        dt = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")
        return time.mktime(dt.timetuple())
    except ValueError:
        return None

# Fetch and store articles from NewsAPI
def fetch_articles(cur, conn, fetch_limit=25):
    """
    Fetches articles related to movies in the database and stores them.
    Limits: 1 page (25 articles) per movie to prevent rate-limit issues.
    """
    total_articles = 0
    PAGE_SIZE = 25
    headers = {
        'User-Agent': 'NewsAPI-Client/1.0',
        'Accept': 'application/json'
    }

    # Fetch movie IDs and titles
    cur.execute("""
        SELECT id, title 
        FROM Movies 
        WHERE year = 2024
        AND id NOT IN (SELECT movie_id FROM Articles)
        LIMIT ?;
    """, (fetch_limit,))
    movies = cur.fetchall()

    if not movies:
        print("No movies from 2024 found in the database.")
        return

    print(f"Fetching articles for up to {len(movies)} movies...")

    for movie_id, movie_title in movies:
        if total_articles >= fetch_limit:
            break

        print(f"Fetching articles for '{movie_title}'...")
        params = {
            'q': movie_title,
            'apiKey': API_KEY,
            'pageSize': PAGE_SIZE,
            'page': 1  # Limit to 1 page
        }

        try:
            response = requests.get(BASE_URL, params=params, headers=headers)
            print(f"API Response Status Code: {response.status_code}")

            if response.status_code == 429:
                print("Rate limit hit. Sleeping for 60 seconds...")
                time.sleep(60)
                continue

            if response.status_code != 200:
                print(f"Error fetching articles for '{movie_title}': {response.status_code}")
                continue

            articles = response.json().get("articles", [])
            if not articles:
                print(f"No articles found for '{movie_title}'.")
                continue

            for article in articles:
                if total_articles >= fetch_limit:
                    break

                article_title = article.get("title", "").strip()
                source_name = article.get("source", {}).get("name", "").strip()
                published_date = article.get("publishedAt", "")
                article_content = article.get("content", "").strip()

                # Convert published_date to REAL (epoch time)
                published_date_epoch = convert_to_unix_epoch(published_date)
                if not published_date_epoch:
                    continue  # Skip invalid dates

                # Insert or retrieve source_id
                cur.execute("SELECT id FROM Sources WHERE source_name = ?;", (source_name,))
                source_row = cur.fetchone()

                if source_row:
                    source_id = source_row[0]
                else:
                    cur.execute("INSERT INTO Sources (source_name) VALUES (?);", (source_name,))
                    conn.commit()
                    source_id = cur.lastrowid

                # Insert into Articles table
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO Articles (movie_id, article_title, source_id, published_date, article_content)
                        VALUES (?, ?, ?, ?, ?);
                    """, (movie_id, article_title, source_id, published_date_epoch, article_content))
                    conn.commit()
                    total_articles += 1
                    print(f"Inserted article: '{article_title}'")
                except sqlite3.Error as e:
                    print(f"Error inserting article: {e}")

            print("Sleeping for 1 second to avoid rate limits...")
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Network error for '{movie_title}': {e}")
            continue

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"TOTAL ARTICLES INSERTED INTO TABLE: {total_articles}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

# Main function
def main():
    """
    Main function to orchestrate database setup and article fetching.
    """
    conn, cur = setup_articles_table("movies2024.db")
    fetch_articles(cur, conn, fetch_limit=25)
    conn.close()

if __name__ == "__main__":
    main()
