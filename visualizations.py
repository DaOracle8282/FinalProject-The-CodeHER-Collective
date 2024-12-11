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

    # Adjust genre names to stack words vertically
    formatted_genres = ['\n'.join(genre.split()) for genre in genres]

    plt.figure(figsize=(10, 6))
    plt.bar(formatted_genres, counts, color='maroon')  # Add color
    plt.title("Top 5 Movie Genres in 2024", fontweight="bold")
    plt.xlabel("Genres", fontweight="bold")
    plt.ylabel("Number of Movies", fontweight="bold")
    plt.tight_layout()  # Adjust layout to fit everything
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

    plt.figure(figsize=(10, 8))
    plt.barh(genres, counts, color='black')  # Add color
    plt.title("Number of Movies By Genre", fontweight="bold")
    plt.xlabel("Number of Movies", fontweight="bold")
    plt.ylabel("Genres", fontweight="bold")
    plt.gca().invert_yaxis()  # Invert to show highest count on top
    plt.tight_layout()
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

    conn.close()


if __name__ == "__main__":
    main()
