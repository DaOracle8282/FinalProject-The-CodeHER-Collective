import sqlite3
import csv
import os


def calculate_avg_rating_by_table(cur, lookup_table, fk_column, file, header_list):
    """
    Calculates average IMDb ratings using a foreign key to the lookup table.
    """
    query = f"""
       SELECT {lookup_table}.{lookup_table[:-1]} AS name, AVG(Movies.imdb_rating)
       FROM Movies
       JOIN {lookup_table} ON Movies.{fk_column} = {lookup_table}.id
       WHERE Movies.imdb_rating > 0
       GROUP BY {lookup_table}.id
   """
    cur.execute(query)
    results =  cur.fetchall()
    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header_list)
        writer.writerows(results)

    print(f"Data written to {file}")


def total_movies_by_table(cur, lookup_table, fk_column, file, header_list):
    """
    Counts movies for each unique value in the lookup table.
    """
    query = f"""
       SELECT {lookup_table}.{lookup_table[:-1]} AS name, COUNT(*)
       FROM Movies
       JOIN {lookup_table} ON Movies.{fk_column} = {lookup_table}.id
       GROUP BY {lookup_table}.id
   """
    cur.execute(query)
    results =  cur.fetchall()

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header_list)
        writer.writerows(results)

    print(f"Data written to {file}")
  

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
    

    
def articles_and_movies(cur, file):
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



def main():
    """
     Main function for processing data and exporting each dataset to its own CSV file.
    """
    db_path = os.path.join(os.path.dirname(__file__), "movies2024.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    #Step 1: Calculate average ratings by genre
    calculate_avg_rating_by_table(cur, "Genres", "genre_id", "avg_ratings_by_genre.csv", ["Genre", "Average Rating"])

    # Step 2: Calculate average ratings by country
    calculate_avg_rating_by_table(cur, "Places", "place_id", "avg_ratings_by_country.csv", ["Country", "Average Rating"])
    
    # Step 3: Count movies by genre
    total_movies_by_table(cur, "Genres", "genre_id", "movies_by_genre.csv", ["Genre", "Total"])

    # Step 4: Count movies by country
    total_movies_by_table(cur, "Places", "place_id", "movies_by_country.csv", ["Country", "Total"])

    # Step 5: Join movies and soundtracks
    join_movies_and_soundtracks(cur, "movies_with_soundtracks.csv")

    # Step 6: Analyze joined data
    articles_and_movies(cur, "movies_articles_analysis.csv")

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
