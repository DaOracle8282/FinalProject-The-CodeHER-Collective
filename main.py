from omdb import set_up_database, fetch_movies_2024
from query_omdb import calculate_avg_rating_by_table, total_movies_by_table
from spotify import get_token, fetch_soundtrack_data, fetch_soundtrack_songs_data

def main():
    """
    Main function for running the movie data pipeline.
    Orchestrates the fetching of data, analysis, and optional integration with Spotify.
    """
    # Step 1: Set up the database
    db_name = "movies2024.db"  # Database name
    cur, conn = set_up_database(db_name)

    # Step 2: Fetch movies and insert into database
    fetch_movies_2024(cur, conn, max_insert=25)

    # Step 3: Calculate average IMDb ratings by genre
    print("\nAverage IMDb Ratings by Genre:")
    genre_ratings = calculate_avg_rating_by_table(cur, "Genres", "genre_id", "ROUND(AVG(Movies.imdb_rating), 2)")
    for genre, rating in genre_ratings:
        print(f"{genre}: {rating:.2f}")

    # Step 4: Calculate average IMDb ratings by country
    print("\nAverage IMDb Ratings by Country:")
    country_ratings = calculate_avg_rating_by_table(cur, "Places", "place_id", "ROUND(AVG(Movies.imdb_rating), 2)")
    for country, rating in country_ratings:
        print(f"{country}: {rating:.2f}")

    # Step 5: Total movies by genre
    print("\nTotal Movies by Genre:")
    genre_counts = total_movies_by_table(cur, "Genres", "genre_id")
    for genre, count in genre_counts:
        print(f"{genre}: {count} movies")

    # Step 6: Total movies by country
    print("\nTotal Movies by Country:")
    country_counts = total_movies_by_table(cur, "Places", "place_id")
    for country, count in country_counts:
        print(f"{country}: {count} movies")

    # Step 7: Fetch Spotify data (optional step)
    print("\nFetching Spotify Soundtracks and Songs...")
    token = get_token()
    fetch_soundtrack_data(cur, conn, token)
    fetch_soundtrack_songs_data(cur, conn, token)

    # Step 8: Close the database connection
    conn.close()
    print("\nData pipeline executed successfully!")

if __name__ == "__main__":
    main()
