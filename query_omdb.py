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
    Returns a list of tuples: (title, year, soundtrack_name, genre).
    """
    cur.execute("""
        SELECT Movies.title, Movies.year, soundtracks.soundtrack_name, Movies.genre
        FROM Movies
        JOIN Soundtracks ON Movies.id = soundtracks.movie_id
        ORDER BY Movies.year DESC
    """)
    return cur.fetchall()

def calculate_avg_song_length_by_album(cur):
    """
    Calculates the average length of songs for each album.

    Parameters:
    - cur: Database cursor.

    Returns:
    - List of tuples: (soundtrack_name, average_length).
    """
    cur.execute("""
        SELECT soundtracks.soundtrack_name, 
               AVG(
                   (CAST(SUBSTR(song_length, 1, 2) AS INTEGER) * 3600) + 
                   (CAST(SUBSTR(song_length, 4, 2) AS INTEGER) * 60) + 
                   CAST(SUBSTR(song_length, 7, 2) AS INTEGER)
               ) AS avg_length_seconds
        FROM soundtrack_songs
        JOIN soundtracks ON soundtrack_songs.soundtrack_id = soundtracks.id
        GROUP BY soundtracks.soundtrack_name
        ORDER BY avg_length_seconds DESC
    """)
    
    results = cur.fetchall()
    
    # Format average length as HH:MM:SS
    formatted_results = []
    for soundtrack_name, avg_seconds in results:
        hours = int(avg_seconds // 3600)
        minutes = int((avg_seconds % 3600) // 60)
        seconds = int(avg_seconds % 60)
        avg_length = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        formatted_results.append((soundtrack_name, avg_length))
    
    return formatted_results


    
def analyze_joined_data(cur,conn):
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
            GROUP BY Movies.title
              ORDER BY article_count DESC;
        """)
        results = cur.fetchall()
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

    avg_song_lengths = calculate_avg_song_length_by_album(cur)
    print("\nAverage Song Length by Album:")
    for album, avg_length in avg_song_lengths:
        print(f"{album}: {avg_length}") 

    articles_and_imdb_ratings = analyze_joined_data(cur, conn)
    print("\nMovie Articles and Imdb Ratings")
    for movie_title, imdb_rating, article_count in articles_and_imdb_ratings:
        print(f"{movie_title}: {imdb_rating}, {article_count}")


    # Step 4: Write data to a CSV file
    data_to_write = {
    "average_ratings_by_genre": avg_ratings,  # Use tuples directly
    "movie_counts_by_year": movie_counts,    # Use tuples directly
    "movies_with_soundtracks": [
        {"title": title, "year": year, "album_name": album, "genre": genre}
        for title, year, album, genre in joined_data],
    "average_song_lengths": avg_song_lengths,
    "articles_and_imdb_ratings": articles_and_imdb_ratings 


}
    write_to_csv("processed_movies.csv", data_to_write)


    # Close connection
    conn.close()

if __name__ == "__main__":
    main()
