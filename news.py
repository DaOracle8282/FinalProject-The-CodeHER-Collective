import sqlite3
import requests
import csv
import matplotlib.pyplot as plt
import json

# Constants
API_KEY = "b68f1a38b495424992a3176ea0263f11"
BASE_URL = "https://newsapi.org/v2/everything"
DB_NAME = "movies.db"
PAGE_SIZE = 25
MAX_ARTICLES_PER_MOVIE = 100


# Set up the Articles table
def setup_articles_table():

    """
    Sets up the Articles table in the existing database.
    Create the table if it doesn't exist with fields for movie title, article details, and source information.
    Includes error handling to ensure smooth execution even if there are issues connecting to the database.
    """
    try:
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
        """
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error setting up Articles table: {e}")
    finally:
        if conn:
            conn.close()


# Fetch and store articles from NewsAPI
def fetch_articles(cur, conn, movie_title, page_limit):

    """
    Fetches articles related to a specific movie title from NewsAPI and stores them in the database.
    
    Parameters:
    - cur: Database cursor.
    - conn: Database connection.
    - movie_title (str): Title of the movie to search articles for.
    - page_limit (int): Number of pages to fetch from the API.

    Handles duplicate articles by checking for existing records before insertion.
    If the API response fails, prints an error message and stops further requests for the current movie.
    """

    total_articles = 0
    page = 1
    headers = {
        'User-Agent': 'NewsAPI-Client/1.0',  # Add a user-agent for identification
        'Accept': 'application/json'         # Specify that the response should be JSON
    }
    while total_articles < MAX_ARTICLES_PER_MOVIE and page <= page_limit:
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
                except sqlite3.Error as e:
                    print(f"Error inserting article: {e}")

            conn.commit()
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching articles for '{movie_title}': {e}")
            break

#Fetch articles for top movies
def fetch_and_store_articles():
    """
    Fetches movie titles from the existing Movies table
    and fetches related articles from NewsAPI
    """
    try: 
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT title FROM Movies ORDER BY year DESC LIMIT 5")
        movies = [row[0] for row in cur.fetchall()]
        print(f"Fetching articles for movies: {movies}")

        for movie_title in movies:
            fetch_articles(cur, conn, movie_title, page_limit=1)

    except sqlite3.Error as e:
        print(f"Error fetching and storing articles: {e}")
    finally:
        if conn:
            conn.close()

#Analyze article counts per movie
def analyze_article_counts():
    """
    Analyzes the number of articles for each movie.

    Returns:
    - results (list of tuples): Each tuple contains the movie title and its article count.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("""
            SELECT movie_title, COUNT(*) as article_count
            FROM Articles
            GROUP BY movie_title
            ORDER BY article_count DESC;
        """)
        results = cur.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Error analyzing article counts: {e}")
        return []
    finally:
        if conn:
            conn.close()

#Perform database join and analysis
def analyze_joined_data():
    """
    Joins the Movies and Articles tables to analyze data.
    Returns:
    - results (list of tuples): Each tuple contains the movie title, IMDb rating, and article count.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute("""
            SELECT m.title, m.imdb_rating, COUNT(a.id) as article_count
            FROM Movies m
            JOIN Articles a ON m.title = a.movie_title
            GROUP BY m.title
            ORDER BY article_count DESC;
        """)
        results = cur.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Error analyzing joined data: {e}")
        return []
    finally:
        if conn:
            conn.close()

#Write data to a file
def write_to_file(data, filename, file_format="csv"):
    """
    Writes analyzed data to a specified file in CSV or JSON format.
    Parameters:
    - data (list of tuples): Data to be written, where each tuple contains fields (ex: Movie Title, IMDb Rating, Article Count).
    - filename (str): Name of the output file.
    - file_format (str): Format of the file, either 'csv' or 'json'.
    
    Purpose:
    Thid function outputs analyzed data for external use, enabling further processing or presentation.
    In CSV format, the file includes headers to describe the fields.
    """
    try:
        if file_format == "csv":
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Movie Title", "IMDb Rating", "Article Count"])
                writer.writerows(data)
        elif file_format == "json":
            with open(filename, "w", encoding="utf-8") as jsonfile:
                json.dump(data, jsonfile, indent=4)
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")

#Create visualizations
def create_visualizations(article_counts, joined_data):
    """
    Creates visualizations for article counts and joined data.
    
    Parameters:
    - article_counts (lists of tuples): Article counts per movie.
    - joined_data (list of tuples): Joined data including IMDb ratings and article counts.
    """
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
    setup_articles_table()
    fetch_and_store_articles()

    #Analyze and visualize data
    article_counts = analyze_article_counts()
    joined_data = analyze_joined_data()

    write_to_file(joined_data, "joined_data.csv", file_format="csv")
    create_visualizations(article_counts, joined_data)

if __name__ == "__main__":
    main()