import sqlite3
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

    plt.figure(figsize=(10, 6))
    plt.bar(genres, counts)
    plt.title("Top 5 Movie Genres")
    plt.xlabel("Genres")
    plt.ylabel("Number of Movies")
    plt.show()

def imdb_ratings_scatter(cur):
    """
    Generates a scatter plot for IMDb ratings versus release year.
    """
    cur.execute("""
        SELECT year, imdb_rating
        FROM Movies
        WHERE imdb_rating > 0
    """)
    data = cur.fetchall()
    years = [row[0] for row in data]
    ratings = [row[1] for row in data]

    plt.figure(figsize=(10, 6))
    plt.scatter(years, ratings, alpha=0.6)
    plt.title("IMDb Ratings vs. Release Year")
    plt.xlabel("Year")
    plt.ylabel("IMDb Rating")
    plt.show()

def genre_pie_chart(cur):
    """
    Generates a pie chart for the proportion of movies in each genre.
    """
    cur.execute("""
        SELECT genre, COUNT(*) as count
        FROM Movies
        GROUP BY genre
    """)
    data = cur.fetchall()
    genres = [row[0] for row in data]
    counts = [row[1] for row in data]

    plt.figure(figsize=(8, 8))
    plt.pie(counts, labels=genres, autopct="%1.1f%%", startangle=140)
    plt.title("Genre Distribution")
    plt.show()

def main():
    """
    Main function to generate visualizations.
    """
    conn = sqlite3.connect("movies.db")
    cur = conn.cursor()

    # Generate visualizations
    genre_bar_chart(cur)
    imdb_ratings_scatter(cur)
    genre_pie_chart(cur)

    conn.close()

if __name__ == "__main__":
    main()
