import sqlite3
import datetime
import matplotlib.pyplot as plt

def genre_bar_chart(cur):
    """
    Generates a bar chart for the top 5 genres with the most movies.
    """
    cur.execute("""
        SELECT genre, COUNT(*) as count
        FROM Movies
        GROUP BY genre
        ORDER BY count DESC
        LIMIT 5
    """)
    data = cur.fetchall()
    genres = [row[0] for row in data]
    counts = [row[1] for row in data]

    # Adjust genre names to stack words vertically
    formatted_genres = ['\n'.join(genre.split()) for genre in genres]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(formatted_genres, counts, color='maroon')
    ax.set_title("Top 5 Movie Genres in 2024", fontweight="bold")
    ax.set_xlabel("Genres", fontweight="bold")
    ax.set_ylabel("Number of Movies", fontweight="bold")
    fig.tight_layout()
    fig.savefig("top_5_movie_genres_2024.png")
    plt.show()

def genre_horizontal_bar_chart(cur):
    """
    Generates a horizontal bar chart for the proportion of movies in each genre.
    """
    cur.execute("""
        SELECT genre, COUNT(*) as count
        FROM Movies
        GROUP BY genre
        ORDER BY count DESC
    """)
    data = cur.fetchall()
    genres = [row[0] for row in data]
    counts = [row[1] for row in data]

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(genres, counts, color='black')  # Add color
    ax.set_title("Number of Movies By Genre", fontweight="bold")
    ax.set_xlabel("Number of Movies", fontweight="bold")
    ax.set_ylabel("Genres", fontweight="bold")
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig("movies_by_genre.png")
    plt.show()


def parse_duration(duration_str):
    # Duration string is in "HH:MM:SS" format
    hrs, mins, secs = map(int, duration_str.split(':'))
    return datetime.timedelta(hours=hrs, minutes=mins, seconds=secs)

def seconds_from_timedelta(td):
    return td.total_seconds()

def longest_movie_soundtracks_chart(cur): 
    cur.execute("""SELECT soundtracks.movie_title, soundtracks.total_duration
                FROM soundtracks
                JOIN Movies ON soundtracks.movie_id =Movies.id
                ORDER BY soundtracks.total_duration DESC
                LIMIT 5""")
    results = cur.fetchall()
    movie_titles = [row[0] for row in results]
    durations = [parse_duration(row[1]) for row in results]

    # Convert timedelta to string format for display purposes
    duration_strings = [str(duration) for duration in durations]
    
    # Prepare duration in seconds for plotting
    duration_in_seconds = [seconds_from_timedelta(duration) for duration in durations]

    fig, ax = plt.subplots(figsize=(12,8))
    ax.bar(movie_titles, duration_in_seconds, color='green')
    ax.set(xlabel="Movie Title", ylabel="Length (in Seconds)", title="2024 Movies(with Movie in the Title) with the Longest Soundtracks")
    fig.autofmt_xdate(rotation=20)
    fig.savefig("longest_movie_soundtracks.png")
    plt.show()
    

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

def main():
    """
    Main function to generate visualizations.
    """
    conn = sqlite3.connect("movies.db")
    cur = conn.cursor()

    # Generate visualizations
    genre_bar_chart(cur)
    genre_horizontal_bar_chart(cur)
    longest_movie_soundtracks_chart(cur)
    articles_per_movie_chart(cur)
    imdb_ratings_and_articles(cur)
    conn.close()


if __name__ == "__main__":
    main()
