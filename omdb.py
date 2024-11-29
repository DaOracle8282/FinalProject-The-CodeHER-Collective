
import requests
import sqlite3
import os

# Step 1: Set Up the Database
def set_up_database(db_name):
    """
    Sets up the SQLite database and creates the Movies table.

    Parameters:
    - db_name (str): Name of the SQLite database file.

    Returns:
    - cursor, connection: Database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            year INTEGER,
            genre TEXT,
            director TEXT,
            imdb_rating REAL,
            box_office INTEGER
        )
    """)
    conn.commit()
    return cur, conn

# Step 2: Fetch Movie Data by Keyword
def fetch_movie_data_by_keyword(keyword="awards"):
    """
    Fetches data from the OMDb API using a keyword search to retrieve up to 100 movies.

    Parameters:
    - keyword (str): The search keyword to fetch movie data. Default is "awards".

    Returns:
    - list: List of movie data dictionaries.
    """
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Your specific API key
    movies_data = []
    total_results = 0

    for page in range(1, 11):  # OMDb API allows up to 10 pages (10 results per page)
        response = requests.get(base_url, params={"s": keyword, "page": page, "apikey": api_key})
        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                movies = data.get("Search", [])
                movies_data.extend(movies)
                total_results += len(movies)
                if total_results >= 100:  # Stop if we’ve fetched at least 100 items
                    break
            else:
                print(f"No more results found for keyword: {keyword}")
                break
        else:
            print(f"Error fetching data for keyword '{keyword}': {response.status_code}")
            break

    return movies_data[:100]  # Ensure we only return up to 100 items

# Step 3: Store Movie Data from Search
def store_movie_data_from_search(movies_data, cur, conn):
    """
    Stores movie data from a search-based API fetch into the database.

    Parameters:
    - movies_data (list): List of movie search results dictionaries.
    - cur: SQLite database cursor.
    - conn: SQLite database connection.

    Returns:
    - None
    """
    for movie in movies_data:
        try:
            # Fetch full details for each movie using its IMDb ID
            movie_id = movie.get("imdbID")
            full_movie_data = requests.get(
                "http://www.omdbapi.com/",
                params={"i": movie_id, "apikey": "25781136"}
            ).json()

            # Handle missing data by replacing 'N/A' with None or defaults
            title = full_movie_data.get("Title", "N/A")
            year = full_movie_data.get("Year", "0")
            genre = full_movie_data.get("Genre", "N/A")
            director = full_movie_data.get("Director", "N/A")
            imdb_rating = full_movie_data.get("imdbRating", "0.0")
            box_office = full_movie_data.get("BoxOffice", "0").replace("$", "").replace(",", "")

            # Convert fields to appropriate types or default values
            year = int(year) if year.isdigit() else None
            imdb_rating = float(imdb_rating) if imdb_rating != "N/A" else None
            box_office = int(box_office) if box_office.isdigit() else None

            # Insert the cleaned data into the database
            cur.execute("""
                INSERT OR IGNORE INTO Movies (title, year, genre, director, imdb_rating, box_office)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (title, year, genre, director, imdb_rating, box_office))

        except Exception as e:
            print(f"Error storing movie: {e}")

    conn.commit()


# Step 4: Query Data from the Database
def query_movies(cur):
    """
    Queries the Movies table for data.

    Parameters:
    - cur: SQLite database cursor.

    Returns:
    - list: List of tuples containing queried movie data.
    """
    cur.execute("""
        SELECT title, genre, imdb_rating, box_office
        FROM Movies
        ORDER BY imdb_rating DESC
    """)
    return cur.fetchall()

def main():
    """
    Main function for testing the OMDb-specific code.
    """
    # Step 1: Set up the database
    cur, conn = set_up_database("movies.db")  # Create or connect to the Movies database

    # Step 2: Fetch and store movie data using the "awards" keyword
    print("Fetching movie data...")
    movies_data = fetch_movie_data_by_keyword("awards")  # Fetch up to 100 movies
    print(f"Fetched {len(movies_data)} movies. Storing data in the database...")
    store_movie_data_from_search(movies_data, cur, conn)  # Store the data in the database
    print("Data successfully stored!")

    # Step 3: Query the database
    print("Querying the database...")
    movies = query_movies(cur)  # Retrieve all stored movies sorted by IMDb rating
    print(f"Retrieved {len(movies)} movies from the database.")
    
    # Print the top 10 movies for verification
    print("\nTop 10 Movies by IMDb Rating:")
    for movie in movies[:10]:
        print(movie)

    # Step 4: Close the database connection
    conn.close()
    print("Database connection closed.")

if __name__ == "__main__":
    main()
