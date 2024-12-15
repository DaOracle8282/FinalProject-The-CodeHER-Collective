import sqlite3
import matplotlib.pyplot as plt
import datetime

def calculate_aggregate_by_table(cur, lookup_table, fk_column, aggregate="COUNT(*)"):
    """
    General function to calculate aggregates for movies using a lookup table.
    """
    query = f"""
        SELECT {lookup_table[:-1]} AS name, {aggregate}
        FROM Movies
        JOIN {lookup_table} ON Movies.{fk_column} = {lookup_table}.id
        GROUP BY {lookup_table}.id
    """
    cur.execute(query)
    
    return {row[0]: row[1] for row in cur.fetchall()}

def plot_bar_chart(data, title, xlabel, ylabel, fig_name):
    """
    Plots a bar chart with custom colors, bold titles, and data annotations.
    """
    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 6))
    
    # Use a color palette
    colors = plt.cm.coolwarm(range(len(data)))  # Color map with distinct colors
    
    # Create the bar chart
    bars = plt.bar(data.keys(), data.values(), color=colors)
    
    # Add bold title and labels
    plt.title(title, fontsize=14, fontweight='bold')  # Bold title
    plt.xlabel(xlabel, fontsize=12, fontweight='bold')  # Bold x-axis label
    plt.ylabel(ylabel, fontsize=12, fontweight='bold')  # Bold y-axis label
    
    # Rotate x-ticks for better readability
    plt.xticks(rotation=45, ha="right", fontsize=10)
    
    # Annotate bars with values
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, f'{yval}', 
                 ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Add a tight layout to improve spacing
    plt.tight_layout()
    plt.savefig(fig_name)
    plt.show()

def longest_movie_soundtracks_chart(cur): 
    cur.execute("""SELECT Soundtracks.movie_title, Soundtracks.total_duration
                FROM Soundtracks
                JOIN Movies ON Soundtracks.movie_id =Movies.id
                ORDER BY Soundtracks.total_duration DESC
                LIMIT 5""")
    results = cur.fetchall()
    movie_titles = [row[0] for row in results]
    durations = [row[1] for row in results]

    fig, ax = plt.subplots(figsize=(12,8))
    ax.bar(movie_titles, durations, color='green')
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
            SELECT Movies.title, COUNT(Articles.id) as article_count
            FROM Movies 
            JOIN Articles  ON Movies.id = Articles.movie_id
            GROUP BY Movies.title
            ORDER BY article_count DESC
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
            JOIN Articles  ON Movies.id = Articles.movie_id
            GROUP BY Movies.title
            ORDER BY article_count DESC
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

def longest_movie_soundtracks_chart(cur): 
    cur.execute("""SELECT soundtracks.movie_title, soundtracks.total_duration
                FROM soundtracks
                JOIN Movies ON soundtracks.movie_id =Movies.id
                ORDER BY soundtracks.total_duration DESC
                LIMIT 5""")
    results = cur.fetchall()
    movie_titles = [row[0] for row in results]
    durations = [row[1]for row in results]
    
    fig, ax = plt.subplots(figsize=(12,8))
    ax.bar(movie_titles, durations, color='green')
    ax.set(xlabel="Movie Title", ylabel="Length (in Seconds)", title="2024 Movies (with Movie in the Title) with the Longest Soundtracks")
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
            SELECT Movies.title, COUNT(Articles.id) as article_count
            FROM Movies 
            JOIN Articles  ON Movies.id = Articles.movie_id
            GROUP BY Movies.id
            ORDER BY article_count DESC
        """)
    article_counts = cur.fetchall()
    # Bar chart: Articles per movie
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

def imdb_ratings_and_articles(cur):
    cur.execute("""
            SELECT Movies.title, Movies.imdb_rating, COUNT(Articles.id) as article_count
            FROM Movies 
            JOIN Articles  ON Movies.id = Articles.movie_id
            GROUP BY Movies.title
            ORDER BY article_count DESC
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
    Main function for generating visualizations.
    """
    db_name = "movies2024.db"
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Total movies by genre
    print("Visualizing total movies by genre...")
    genre_counts = calculate_aggregate_by_table(cur, "Genres", "genre_id")
    plot_bar_chart(genre_counts, "Total Movies by Genre", "Genre", "Number of Movies", "movies_by_genre.png")

    # Total movies by country
    print("Visualizing total movies by country...")
    country_counts = calculate_aggregate_by_table(cur, "Places", "place_id")
    plot_bar_chart(country_counts, "Total Movies by Country", "Country", "Number of Movies", "movies_by_country.png")

    # Average ratings by genre
    print("Visualizing average IMDb ratings by genre...")
    avg_genre_ratings = calculate_aggregate_by_table(cur, "Genres", "genre_id", "ROUND(AVG(Movies.imdb_rating), 2)")
    plot_bar_chart(avg_genre_ratings, "Average IMDb Ratings by Genre", "Genre", "Average Rating",  "ratings_by_genre.png")

    # Average ratings by country
    print("Visualizing average IMDb ratings by country...")
    avg_country_ratings = calculate_aggregate_by_table(cur, "Places", "place_id", "ROUND(AVG(Movies.imdb_rating), 2)")
    plot_bar_chart(avg_country_ratings, "Average IMDb Ratings by Country", "Country", "Average Rating", "ratings_by_country.png")

    longest_movie_soundtracks_chart(cur)
    articles_per_movie_chart(cur)
    imdb_ratings_and_articles(cur)
    
    conn.close()

if __name__ == "__main__":
    main()
