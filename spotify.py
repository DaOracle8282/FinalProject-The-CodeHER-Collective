import sqlite3
import spotipy
import spotipy.oauth2 as oauth2
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import sys
from json.decoder import JSONDecodeError
from omdb import set_up_database, fetch_movies_by_year
import os

def set_up_database(db_name):
    """
    Sets up the SQLite database and creates the Movies table.

    Parameters:
    - db_name (str): Name of the SQLite database file.

    Returns:
    - cursor, connection: Database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    # Create the Movies table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            year INTEGER,
            genre TEXT,
            country TEXT,
            imdb_rating REAL
        )
    """)
    conn.commit()
    return cur, conn
#Step 1: Set up connection to Spotipy
def get_token():
   CLIENT_ID = "cdc220444d2a42f5a7c4472fbe862667"
   CLIENT_SECRET = "7f72898d496246ea978f8886753a3557"

   sp_oauth = oauth2.SpotifyClientCredentials(client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET)
   token_info = sp_oauth.get_access_token(as_dict=False)
   return token_info
 


#Step 2: Create soundtrack table in existing Movies database
def create_soundtrack_and_song_tables(db_name):
   """
  creates the soundtrack table.


   Parameters:
   - db_name (str): Name of the SQLite database file.


   Returns:
   - cursor, connection: Database cursor and connection objects.
   """
   path = os.path.dirname(os.path.abspath(__file__))
   conn = sqlite3.connect(os.path.join(path, db_name))
   cur = conn.cursor()
   cur.execute("""CREATE TABLE IF NOT EXISTS soundtracks ( 
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               movie_title TEXT UNIQUE, 
               movie_id INTEGER,
               soundtrack_name TEXT UNIQUE, 
               genre TEXT,
               FOREIGN KEY (movie_id) REFERENCES movie(id)
               )
                """)
   
   cur.execute("""CREATE TABLE IF NOT EXISTS soundtrack_songs (
               id INTEGER PRIMARY KEY,
               song_title TEXT,
               soundtrack_id INTEGER,
               FOREIGN KEY (soundtrack_id) REFERENCES soundtracks(id))
               """)
   conn.commit()
   return cur, conn



#Step 3: Request movie soundtrack data from Spotipy and store in soundtrack table
def fetch_soundtrack_data(cur, conn, token, movies_list):
    offset = 0
    limit = 100

    # Count existing soundtracks in the database
    cur.execute("SELECT COUNT(*) FROM soundtracks")
    soundtracks_count = cur.fetchone()[0]

    if soundtracks_count >= limit:
        print(f"Database already contains {soundtracks_count} soundtracks. Limit reached.")
        return

    while soundtracks_count < limit:
        sp = spotipy.Spotify(auth=token)
        try:
            for title in movies_list:
            # Search for albums by title
                results = sp.search(q=title, type="album", limit=1, offset=offset)
                albums = results["albums"]["items"]

                if not albums:
                    print(f"Soundtrack not found for {title}.")
                    continue

                else:
                    for album in albums: 
                        movie_title =title
                        soundtrack_name = album["name"]
                        #artists = ", ".join(artist["name"] for artist in album["artists"])
                        genre = "Soundtrack"  # Assuming all are soundtracks

                        # Fetch the movie_id from the Movies table
                        cur.execute("SELECT id FROM Movies WHERE title = ?", (movie_title,))
                        movie_id_result = cur.fetchone()

                        if movie_id_result:
                            movie_id = movie_id_result[0]
                # Insert into the database
                            try:
                                cur.execute("""
                                    INSERT OR IGNORE INTO Soundtracks (movie_title, movie_id, soundtrack_name, genre)
                                    VALUES (?,?, ?, ?)
                                """, (movie_title, soundtrack_name, movie_id, genre))
                                soundtracks_count += 1
                                print(f"Inserted: {soundtrack_name}")

                            except Exception as e:
                                print(f"Error inserting soundtrack: {soundtrack_name}. Error: {e}")
            
            # Increase offset for the next batch
                offset += 50
        except Exception as e:
            print(f"Error fetching data from Spotify: {e}")
            break



#Step 4: Run a query on Soundtracks table
def soundtrack_query(cur):
    cur.execute("""
        SELECT movie_title, artists, album_name, genre
        FROM Soundtracks
        ORDER BY movie_title ASC
    """)
    results = cur.fetchall()
    for row in results:
        print(row)


#Step 5: Define main function
def main():
    token = get_token()
    cur, conn = set_up_database("movies.db")
    movies = fetch_movies_by_year(cur, conn, start_year=2015, limit=25)
    create_soundtrack_and_song_tables("movies.db")
    fetch_soundtrack_data(cur, conn, token, movies)
    # Example movie titles
    conn.close()
                                                                                                                                                              
'''
   soundtrack_query(cur)
'''

# Step 6: Run the main function
if __name__ == "__main__":
   main()


