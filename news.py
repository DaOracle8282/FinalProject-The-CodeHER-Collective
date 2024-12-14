import sqlite3
import requests
import matplotlib.pyplot as plt
import json
import os

# Constants
API_KEY = "80dd3b4afaa84da7b3b9be836471ea07"
BASE_URL = "https://newsapi.org/v2/everything"



# Set up the Articles table
def setup_articles_table(db_name):

    """
    Sets up the Articles table in the existing database.
    Create the table if it doesn't exist with fields for movie title, article details, and source information.
    Includes error handling to ensure smooth execution even if there are issues connecting to the database.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    try:
        cursor = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_title INTEGER,
                article_title TEXT,
                source_name TEXT,
                published_date TEXT,
                article_content TEXT,
                UNIQUE(movie_title, article_title, published_date)
                FOREIGN KEY (movie_id) REFERENCES Movies(id)
            );
        """
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error setting up Articles table: {e}")
    return conn,cur


# Fetch and store articles from NewsAPI
def fetch_articles(cur, conn, fetch_limit=25):

    """
    Fetches articles related to a specific movie title from NewsAPI and stores them in the database.
    
    Parameters:
    - cur: Database cursor.
    - conn: Database connection.
    - fetch_limit: limits amount of articles retrieved at one time to 25.

    Handles duplicate articles by checking for existing records before insertion.
    If the API response fails, prints an error message and stops further requests for the current movie.
    """

    total_articles = 0
    page = 1
    PAGE_SIZE = 25
    headers = {
        'User-Agent': 'NewsAPI-Client/1.0',  # Add a user-agent for identification
        'Accept': 'application/json'         # Specify that the response should be JSON
    }
    cur.execute("""SELECT id, title 
                FROM Movies 
                WHERE year = 2024
                AND Movies.title NOT IN (SELECT Articles.movie_title FROM Articles)""")
    movies = cur.fetchall()
    if not movies:
        print("No movies from 2024 found in the database.")
        return

    print("Fetching soundtracks for movies from 2024...")

    for id, movie_title in movies:
        
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

                    cur.execute("""
                    SELECT COUNT(*) FROM Articles
                    WHERE movie_title = ? AND article_title = ? AND published_date = ?;
                """, (movie_title, article_title, published_date))
                    if cur.fetchone()[0] > 0:
                        continue

                    try:
                        cur.execute("""
                        INSERT OR IGNORE INTO Articles (movie_title, article_title, source_name, published_date, article_content)
                        VALUES (?, ?, ?, ?, ?)
                        """, (movie_title, article_title, source_name, published_date, article_content))
                        total_articles += 1
                        print(f"Inserted: {article_title}")
                        conn.commit()
                    except sqlite3.Error as e:
                        print(f"Error inserting article: {e}")

                page += 1
            except requests.exceptions.RequestException as e:
                print(f"Network error while fetching articles for '{movie_title}': {e}")
                break

#Analyze article counts per movie
def analyze_article_counts(con, cur):
    """
    Analyzes the number of articles for each movie.

    Returns:
    - results (list of tuples): Each tuple contains the movie title and its article count.
    """
    try:
        cur.execute("""
            SELECT movie_title, COUNT(*) as article_count
            FROM Articles
            GROUP BY movie_title
            ORDER BY article_count DESC;
        """)
        return cur.fetchall()
    except sqlite3.Error as e:
        print(f"Error analyzing article counts: {e}")
        return []
    

#Perform database join and analysis
def analyze_joined_data(con,cur):
    """
    Joins the Movies and Articles tables to analyze data.
    Returns:
    - results (list of tuples): Each tuple contains the movie title, IMDb rating, and article count.
    """
    try:
        
        cur.execute("""
            SELECT Movies.title, Movies.imdb_rating, COUNT(Articles.id) as article_count
            FROM Movies 
            JOIN Articles  ON Movies.title = Articles.movie_title
            GROUP BY Movie.title
            ORDER BY article_count DESC;
        """)
        results = cur.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Error analyzing joined data: {e}")
        return []


#Create visualizations
def articles_per_movie_chart(cur):
    """
    Creates visualizations for article counts and joined data.
    
    Parameters:
    - article_counts (lists of tuples): Article counts per movie.
    - joined_data (list of tuples): Joined data including IMDb ratings and article counts.
    """
    cur.execute("""
            SELECT movie_title, COUNT(*) as article_count
            FROM Articles
            GROUP BY movie_title
            ORDER BY article_count DESC;
        """)
    article_counts = cur.fetchall()
    # Bar chart: Articles per movie
    movie_titles = [row[0] for row in article_counts]
    counts = [row[1] for row in article_counts]

    plt.figure(figsize=(10, 6))
    plt.bar(movie_titles, counts, color="skyblue", label="Article Count")
    plt.title("Number of Articles per Movie\n(Data fetched dynamically)")
    plt.xlabel("Movie Title")
    plt.ylabel("Article Count")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig("articles_per_movie.png")
    plt.show()


def imdb_ratings_and_articles(cur):
    cur.execute("""
            SELECT Movies.title, Movies.imdb_rating, COUNT(Articles.id) as article_count
            FROM Movies 
            JOIN Articles  ON Movies.title = Articles.movie_title
            GROUP BY Movies.title
            ORDER BY article_count DESC;
        """)
    joined_data = cur.fetchall()
    # Scatter plot: IMDb rating vs. article count
    movie_titles = [row[0] for row in joined_data]
    imdb_ratings = [row[1] for row in joined_data]
    article_counts = [row[2] for row in joined_data]

    plt.figure(figsize=(10, 6))
    plt.scatter(imdb_ratings, article_counts, color="green", alpha=0.7, label="Movies")
    plt.title("IMDb Rating vs. Article Count")
    plt.xlabel("IMDb Rating")
    plt.ylabel("Article Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig("imdb_vs_articles.png")
    plt.show()

# Main function
def main():
    """
    Main function to orchestrate the process of setting up the database,
    fetching articles, analyzing data, and creating visualizations.
    """
    db_name = "movies2024.db"
    conn,cur = setup_articles_table(db_name)
    fetch_articles(cur, conn, fetch_limit=25)
    
    #Analyze and visualize data
    articles_per_movie_chart(cur)
    imdb_ratings_and_articles(cur)
    conn.close()

if __name__ == "__main__":
    main()