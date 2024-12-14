import sqlite3
import csv
import os

def calculate_avg_rating_by_genre(cur, file):
    """
    Calculates the average IMDb rating for each genre.
    Returns a list of tuples: (genre, average_rating).
    """
    cur.execute("""
        SELECT genre, AVG(imdb_rating) as avg_rating
        FROM Movies
        WHERE imdb_rating > 0
        GROUP BY genre
        ORDER BY avg_rating DESC
    """)
    results = cur.fetchall()

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Genre", "Average Rating"])
        writer.writerows(results)

    print(f"Average IMDb ratings by genre written to {file}")
    return results

def count_movies_by_year(cur, file):
    """
    Counts the number of movies for each year.
    Returns a list of tuples: (year, count).
    """
    cur.execute("""
        SELECT year, COUNT(*) as movie_count
        FROM Movies
        GROUP BY year
        ORDER BY year DESC
    """)
    results = cur.fetchall()

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Year", "Movie Count"])
        writer.writerows(results)
    print(f"Movie counts by year written to {file}")
    return results

def join_movies_and_soundtracks(cur, file):
    """
    Joins the Movies and Soundtracks tables on the title.
    Returns a list of tuples: (title, year, soundtrack_name, genre).
    """
    cur.execute("""
        SELECT Movies.title, Movies.year, soundtracks.soundtrack_name, Movies.genre
        FROM Movies
        JOIN Soundtracks ON Movies.id = soundtracks.movie_id
        ORDER BY Movies.year DESC
                

    """)
    results = cur.fetchall()


    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Title", "Year", "Soundtrack Name", "Genre"])
        writer.writerows(results)
    print(f"Movies with soundtracks written to {file}")
    return results
    

    
def analyze_joined_data(cur, file):
    """
    Counts the 
    Returns:
    - results (list of tuples): Each tuple contains the movie title, IMDb rating, and article count.
    """
    try:
        
        cur.execute("""
            SELECT Movies.title, Movies.imdb_rating, COUNT(Articles.id) as article_count
            FROM Movies 
            JOIN Articles  ON Movies.title = Articles.movie_title
            GROUP BY Movies.title
              ORDER BY article_count DESC;
        """)
        results = cur.fetchall()

        with open(file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Movie Title", "IMDb Rating", "Article Count"])
            writer.writerows(results)

        print(f"Joined data analysis written to {file}")
        return results
    except sqlite3.Error as e:
        print(f"Error analyzing joined data: {e}")
        return []

import csv

def write_to_csv(filename, data):
    """
    Writes the processed data to a CSV file.
    """
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        
        # Write headers for each section
        writer.writerow(["Average IMDb Rating by Genre"])
        writer.writerow(["Genre", "Average Rating"])
        for genre, rating in data["average_ratings_by_genre"]:
            writer.writerow([genre, rating ])
        
        writer.writerow([])  # Empty line
        writer.writerow(["Movie Counts by Year"])
        writer.writerow(["Year", "Count"])
        for year, count in data["movie_counts_by_year"]:
            writer.writerow([year, count])
        
        writer.writerow([])  # Empty line
        writer.writerow(["Movies with Soundtracks"])
        writer.writerow(["Title", "Year", "Album Name", "Genre"])
        for row in data["movies_with_soundtracks"]:
            writer.writerow([row["title"], row["year"], row["album_name"], row["genre"]])
            
        writer.writerow([])  # Empty line
        writer.writerow(["Average Song Length by Album"])
        writer.writerow(["Album Name", "Average Length"])
        for album_name, avg_length in data["average_song_lengths"]:
            writer.writerow([album_name, avg_length])

        writer.writerow([])  # Empty line
        writer.writerow(["Movies, IMDB Rating, and Article Count"])
        writer.writerow(["Movie Title", "IMDb Rating", "Article Count"])
        for movie_title, imdb_rating, article_count in data["articles_and_imdb_ratings"]:
            writer.writerow([movie_title, imdb_rating, article_count])

    print(f"Data successfully written to {filename}")


def main():
    """
     Main function for processing data and exporting each dataset to its own CSV file.
    """
    db_path = os.path.join(os.path.dirname(__file__), "movies2024.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Step 1: Calculate average ratings by genre
    calculate_avg_rating_by_genre(cur, "avg_ratings_by_genre.csv")

    # Step 2: Count movies by year
    count_movies_by_year(cur, "movie_counts_by_year.csv")

    # Step 3: Join movies and soundtracks
    join_movies_and_soundtracks(cur, "movies_with_soundtracks.csv")

    # Step 4: Analyze joined data
    analyze_joined_data(cur, "movies_articles_analysis.csv")

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
