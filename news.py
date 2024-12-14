import sqlite3
import requests
import matplotlib.pyplot as plt
import os

# Constants
API_KEY = "b68f1a38b495424992a3176ea0263f11"
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
        # Sources table (lookup table for source_name)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT UNIQUE
            )
        """)

        # Articles table with source_id as a foreign key
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                article_title TEXT,
                source_id INTEGER,
                published_date TEXT,
                article_content TEXT,
                UNIQUE(article_title, published_date),
                FOREIGN KEY (movie_id) REFERENCES Movies(id),
                FOREIGN KEY (source_id) REFERENCES Sources(id)
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
    Ensures no duplicate sources or articles are inserted.
    """
    total_articles = 0
    page = 1
    PAGE_SIZE = 25
    headers = {
        'User-Agent': 'NewsAPI-Client/1.0',
        'Accept': 'application/json'
    }

    # Fetch movies without corresponding articles
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
                if response.status_code != 200:
                    print(f"Error fetching articles for '{movie_title}': {response.status_code}")
                    break

                articles = response.json().get("articles", [])
                if not articles:
                    print(f"No more articles found for '{movie_title}'.")
                    break

                for article in articles:
                    article_title = article.get("title", "").strip()
                    source_name = article.get("source", {}).get("name", "").strip()
                    published_date = article.get("publishedAt", "").strip()
                    article_content = article.get("content", "").strip()

                    # Skip if essential data is missing
                    if not article_title or not source_name or not published_date:
                        continue

                    # Check if source already exists, get or insert source_id
                    cur.execute("SELECT id FROM Sources WHERE source_name = ?", (source_name,))
                    source_row = cur.fetchone()
                    if source_row:
                        source_id = source_row[0]
                    else:
                        cur.execute("INSERT INTO Sources (source_name) VALUES (?)", (source_name,))
                        conn.commit()
                        source_id = cur.lastrowid

                    # Insert into Articles table if it's not a duplicate
                    cur.execute("""
                        INSERT OR IGNORE INTO Articles (movie_id, article_title, source_id, published_date, article_content)
                        VALUES (?, ?, ?, ?, ?)
                    """, (movie_id, article_title, source_id, published_date, article_content))

                    total_articles += 1
                    print(f"Inserted article: {article_title}")

                conn.commit()
                page += 1

            except (sqlite3.Error, requests.exceptions.RequestException) as e:
                print(f"Error processing articles for '{movie_title}': {e}")
                break

    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"TOTAL ARTICLES INSERTED INTO TABLE: {total_articles}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


# Analyze article counts per movie
def analyze_article_counts(cur):
    """
    Analyzes the number of articles for each movie.
    """
    try:
        cur.execute("""
            SELECT Movies.title, COUNT(Articles.id) as article_count
            FROM Movies
            JOIN Articles ON Movies.id = Articles.movie_id
            GROUP BY Movies.title
            ORDER BY article_count DESC;
        """)
        return cur.fetchall()
    except sqlite3.Error as e:
        print(f"Error analyzing article counts: {e}")
        return []


# Create visualizations
def articles_per_movie_chart(cur):
    """
    Creates a bar chart showing the number of articles per movie.
    """
    cur.execute("""
        SELECT Movies.title, COUNT(Articles.id) as article_count
        FROM Movies
        JOIN Articles ON Movies.id = Articles.movie_id
        GROUP BY Movies.title
        ORDER BY article_count DESC;
    """)
    article_counts = cur.fetchall()
    movie_titles = [row[0] for row in article_counts]
    counts = [row[1] for row in article_counts]

    plt.figure(figsize=(10, 6))
    plt.bar(movie_titles, counts, color="skyblue", label="Article Count")
    plt.title("Number of Articles per Movie")
    plt.xlabel("Movie Title")
    plt.ylabel("Article Count")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig("articles_per_movie.png")
    plt.show()


# Main function
def main():
    """
    Main function to orchestrate the process of setting up the database,
    fetching articles, analyzing data, and creating visualizations.
    """
    # Setup database
    conn, cur = setup_articles_table("movies2024.db")

    # Fetch articles from NewsAPI
    fetch_articles(cur, conn, fetch_limit=25)

    # Analyze and display article counts
    print("\nArticle Counts by Movie:")
    article_counts = analyze_article_counts(cur)
    for movie_title, count in article_counts:
        print(f"{movie_title}: {count} articles")

    # Generate visualizations
    articles_per_movie_chart(cur)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()