from omdb import set_up_database, fetch_movies_2024
from query_omdb import query_shared_movies_and_soundtracks, calculate_average_rating, genre_count
from spotify import fetch_spotify_data, get_token

def main():
    # Step 1: Set up the database
    db_name = "movies.db"
    cur, conn = set_up_database(db_name)

    # Step 2: Fetch movies and insert into database
    fetch_movies_2024(cur, conn, max_total=100, fetch_limit=25)

    # Step 3: Query for shared movies with soundtracks
    print("\nMovies with Soundtracks:")
    shared_data = query_shared_movies_and_soundtracks(cur)
    for movie, album in shared_data:
        print(f"Movie: {movie}, Soundtrack: {album}")

    # Step 4: Calculate the average IMDb rating
    avg_rating = calculate_average_rating(cur)
    print(f"\nAverage IMDb Rating: {avg_rating:.2f}")

    # Step 5: Display genre counts
    print("\nGenre Counts:")
    for genre, count in genre_count(cur):
        print(f"{genre}: {count} movies")

    # Step 6: Fetch Spotify data
    token = get_token()
    fetch_spotify_data(cur, conn, token, limit=25)

    # Step 7: Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
