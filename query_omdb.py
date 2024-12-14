import sqlite3
import csv

def calculate_avg_rating_by_table(cur, lookup_table, fk_column):
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
    return cur.fetchall()

def total_movies_by_table(cur, lookup_table, fk_column):
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
    return cur.fetchall()

def count_movies_by_year(cur):
    """
    Counts the number of movies grouped by year.
    """
    query = """
        SELECT year, COUNT(*) 
        FROM Movies
        GROUP BY year
        ORDER BY year DESC
    """
    cur.execute(query)
    return cur.fetchall()

def write_to_csv(filename, data_dict):
    """
    Writes query results to a CSV file in a structured format.
    """
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

    print(f"Data successfully written to {filename}.")

def main():
    """
    Main function to test querying logic and write data to CSV.
    """
    db_path = "movies2024.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

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

    print("\nAverage IMDb Ratings by Country:")
    for country, rating in avg_ratings_country:
        print(f"{country}: {rating:.2f}")

    print("\nTotal Movies by Genre:")
    for genre, count in total_movies_genre:
        print(f"{genre}: {count} movies")

    print("\nTotal Movies by Country:")
    for country, count in total_movies_country:
        print(f"{country}: {count} movies")

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

    conn.close()

if __name__ == "__main__":
    main()
