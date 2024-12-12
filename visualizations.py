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

    conn.close()


if __name__ == "__main__":
    main()
