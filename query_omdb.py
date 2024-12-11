import sqlite3
import csv
import os

def calculate_avg_rating_by_genre(cur):
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
    return cur.fetchall()

def count_movies_by_year(cur):
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
    return cur.fetchall()

def join_movies_and_soundtracks(cur):
    """
    Joins the Movies and Soundtracks tables on the title.
    Returns a list of tuples: (title, year, album_name, genre).
    """
    cur.execute("""
        SELECT Movies.title, Movies.year, Soundtracks.album_name, Movies.genre
        FROM Movies
        JOIN Soundtracks ON Movies.title = Soundtracks.album_name
        ORDER BY Movies.year DESC
    """)
    return cur.fetchall()

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
            writer.writerow([genre, f"{rating:.2f}"])
        
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
        
    print(f"Data successfully written to {filename}")


def main():
    """
    Main function for processing and exporting data.
    """
    db_path = os.path.join(os.path.dirname(__file__), "movies.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Step 1: Calculate average ratings by genre
    avg_ratings = calculate_avg_rating_by_genre(cur)
    print("Average IMDb Rating by Genre:")
    for genre, rating in avg_ratings:
        print(f"{genre}: {rating:.2f}")

    # Step 2: Count movies by year
    movie_counts = count_movies_by_year(cur)
    print("\nMovie Counts by Year:")
    for year, count in movie_counts:
        print(f"{year}: {count}")

    # Step 3: Join movies and soundtracks
    joined_data = join_movies_and_soundtracks(cur)
    print("\nMovies with Soundtracks:")
    for row in joined_data[:10]:  # Display first 10 rows for verification
        print(row)

    # Step 4: Write data to a CSV file
    data_to_write = {
        "average_ratings_by_genre": [{"genre": genre, "avg_rating": rating} for genre, rating in avg_ratings],
        "movie_counts_by_year": [{"year": year, "count": count} for year, count in movie_counts],
        "movies_with_soundtracks": [{"title": title, "year": year, "album_name": album, "genre": genre} 
                                    for title, year, album, genre in joined_data]
    }
    write_to_csv("processed_movies.csv", data_to_write)


    # Close connection
    conn.close()

if __name__ == "__main__":
    main()
