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
<<<<<<< HEAD
        JOIN Soundtracks ON Movies.id = soundtracks.movie_id
        ORDER BY Movies.year DESC
    """)
=======
        JOIN {lookup_table} ON Movies.{fk_column} = {lookup_table}.id
        GROUP BY {lookup_table}.id
    """
    cur.execute(query)
>>>>>>> 99223d73fd5e553b3abb9528ea09bd7419583405
    return cur.fetchall()

def count_movies_by_year(cur):
    """
<<<<<<< HEAD
    Counts the 
    Returns:
    - results (list of tuples): Each tuple contains the movie title, IMDb rating, and article count.
=======
    Counts the number of movies grouped by year.
    """
    query = """
        SELECT year, COUNT(*) 
        FROM Movies
        GROUP BY year
        ORDER BY year DESC
>>>>>>> 99223d73fd5e553b3abb9528ea09bd7419583405
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
<<<<<<< HEAD
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
=======
    with open(filename, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Category", "Name", "Value"])  # CSV Header

        # Write average ratings by genre
        writer.writerow(["Average IMDb Ratings by Genre", "", ""])
        for genre, avg_rating in data_dict["average_ratings_by_genre"]:
            writer.writerow(["Genre", genre, f"{avg_rating:.2f}"])

        # Write average ratings by country
        writer.writerow(["", "", ""])  # Blank row
        writer.writerow(["Average IMDb Ratings by Country", "", ""])
        for country, avg_rating in data_dict["average_ratings_by_country"]:
            writer.writerow(["Country", country, f"{avg_rating:.2f}"])

        # Write total movie counts by genre
        writer.writerow(["", "", ""])
        writer.writerow(["Total Movies by Genre", "", ""])
        for genre, count in data_dict["total_movies_by_genre"]:
            writer.writerow(["Genre", genre, count])

        # Write total movie counts by country
        writer.writerow(["", "", ""])
        writer.writerow(["Total Movies by Country", "", ""])
        for country, count in data_dict["total_movies_by_country"]:
            writer.writerow(["Country", country, count])
>>>>>>> 99223d73fd5e553b3abb9528ea09bd7419583405


def main():
    """
<<<<<<< HEAD
    Main function for processing and exporting data.
=======
    Main function to test querying logic and write data to CSV.
>>>>>>> 99223d73fd5e553b3abb9528ea09bd7419583405
    """
    db_path = os.path.join(os.path.dirname(__file__), "movies2024.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

<<<<<<< HEAD
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
=======
    # Step 1: Fetch query data
    avg_ratings_genre = calculate_avg_rating_by_table(cur, "Genres", "genre_id")
    avg_ratings_country = calculate_avg_rating_by_table(cur, "Places", "place_id")
    total_movies_genre = total_movies_by_table(cur, "Genres", "genre_id")
    total_movies_country = total_movies_by_table(cur, "Places", "place_id")
    movie_counts = count_movies_by_year(cur)

    # Step 2: Display results
    print("\nAverage IMDb Ratings by Genre:")
    for genre, rating in avg_ratings_genre:
        print(f"{genre}: {rating:.2f}")
>>>>>>> 99223d73fd5e553b3abb9528ea09bd7419583405


}
    write_to_csv("processed_movies.csv", data_to_write)


<<<<<<< HEAD
    # Close connection
=======
    print("\nMovie Counts by Year:")
    for year, count in movie_counts:
        print(f"{year}: {count} movies")

    # Step 3: Write results to CSV
    data_to_write = {
        "average_ratings_by_genre": avg_ratings_genre,
        "average_ratings_by_country": avg_ratings_country,
        "total_movies_by_genre": total_movies_genre,
        "total_movies_by_country": total_movies_country,
    }
    write_to_csv("query_movies.csv", data_to_write)

>>>>>>> 99223d73fd5e553b3abb9528ea09bd7419583405
    conn.close()

if __name__ == "__main__":
    main()
