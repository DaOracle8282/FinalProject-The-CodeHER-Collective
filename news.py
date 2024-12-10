import sqlite3
import requests

# Constants
API_KEY = "b68f1a38b495424992a3176ea0263f11"
BASE_URL = "https://newsapi.org/v2/everything"
DB_NAME = "newsapi_project.db"
MOVIES = []
MAX_ARTICLES_PER_MOVIE = 100
PAGE_SIZE = 25

# Create database and table
def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_title TEXT,
            article_title TEXT,
            source_name TEXT,
            published_date TEXT,
            article_content TEXT,
            UNIQUE(movie_title, article_title, published_date)
        );
    """)
    conn.commit()
    conn.close()

# Fetch and store data from NewsAPI
def fetch_articles(movie_title, page_limit):
    total_articles = 0
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for page in range(1, page_limit + 1):
        if total_articles >= MAX_ARTICLES_PER_MOVIE:
            break

        params = {
            'q': movie,
            'apiKey': API_KEY,
            'pageSize': PAGE_SIZE,
            'page': page
        }
        headers = {
            'User-Agent': 'Python NewsAPI Client'
        }
        response = requests.get(BASE_URL, params=params, headers=headers)
        if response.status_code == 200:
            articles = response.json().get('articles', [])
            for article in articles:
                # Normalize fields to avoid subtle duplicates
                article_title = article.get('title', '').strip()
                published_date = article.get('publishedAt', '').strip()
                source_name = article.get('source', {}).get('name', '').strip()
                article_content = article.get('content', '').strip()

                    # Skip duplicates based on normalized fields
                cursor.execute("""
                    SELECT COUNT(*) FROM Articles
                    WHERE movie_title = ? AND article_title = ? AND published_date = ?;
                """, (movie, article_title, published_date))
                if cursor.fetchone()[0] > 0:
                    continue

                try:
                    cursor.execute("""
                        INSERT INTO Articles (movie_title, article_title, source_name, published_date, article_content)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        movie_title, article_title, source_name, published_date, article_content
                    ))
                    total_articles += 1
                    if total_articles >= MAX_ARTICLES_PER_MOVIE:
                        break
                except Exception as e:
                    print(f"Error inserting article: {e}")
        elif response.status_code == 426:
            print(f"Upgrade Required: Unable to fetch page {page} for movie '{movie}'.")
            break
        else:
            print(f"Error fetching page {page} for movie '{movie}'. Status code: {response.status_code}")
            break
    conn.commit()
    conn.close()

def fetch_and_store_articles(movies, page_limit=5):
    for movie in movies:
        print(f"Fetching articles for movie: {movie}")
        fetch_articles(movie, page_limit)

# Analyze data: Number of articles per movie
def analyze_article_counts():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT movie_title, COUNT(*) as article_count
        FROM Articles
        GROUP BY movie_title
        ORDER BY article_count DESC;
    """)
    results = cursor.fetchall()
    conn.close()
    return results

# Analyze trends for the top movie
def analyze_trends(top_movie):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT published_date, COUNT(*) as article_count
        FROM Articles
        WHERE movie_title = ?
        GROUP BY published_date
        ORDER BY published_date;
    """, (top_movie,))
    results = cursor.fetchall()
    conn.close()
    return results

# Main execution
setup_database()

#Prompt user to input movie titles
movie_input = input("Enter movie titles separated by commas: ")
movies = [movie.strip() for movie in movie_input.split(",")]

fetch_and_store_articles(movies, page_limit=5)
article_counts = analyze_article_counts()

if article_counts:
    print("Article Counts per Movie:")
    for row in article_counts:
        print(f"Movie: {row[0]}, Articles: {row[1]}")

    top_movie = article_counts[0][0]
    trends = analyze_trends(top_movie)
    if trends:
        print(f"\nMedia Coverage Trends for {top_movie}:")
        for row in trends:
            print(f"Date: {row[0]}, Articles: {row[1]}")
