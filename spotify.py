import sqlite3
import spotipy
import spotipy.oauth2 as oauth2
import os
"""
Things to complete: 
[X] search for only 25 albums at a time
[X] add only 25 songs at a time
[] filter for genre if possible and create visual for top 5 most popular movie soundtrack genres
[] if not create visual for top 5  movies with the longest album length and/or longest average song length
[] DONE!
"""
#Step 1: Set up connection to Spotipy
def get_token():
    """
    Sets up the connection to the Spotipy API.

    Parameters:
    - None.

    Returns:
    - token_info: token granted to access Spotify API.
    """
    CLIENT_ID = "cdc220444d2a42f5a7c4472fbe862667"
    CLIENT_SECRET = "7f72898d496246ea978f8886753a3557"

    sp_oauth = oauth2.SpotifyClientCredentials(client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET)
    token_info = sp_oauth.get_access_token(as_dict=False)
    return token_info
 


#Step 2: Create soundtrack table in existing Movies database
def create_soundtrack_and_song_tables(db_name):
   """
  creates the soundtrack and soundtrack_songs tables.


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
               total_duration TEXT,
               FOREIGN KEY (movie_id) REFERENCES movie(id)
               )
                """) 
   
   cur.execute("""CREATE TABLE IF NOT EXISTS soundtrack_songs (
               id INTEGER PRIMARY KEY,
               song_title TEXT,
               soundtrack_id INTEGER,
               song_length TEXT,
               FOREIGN KEY (soundtrack_id) REFERENCES soundtracks(id))
               """)
   conn.commit()
   return cur, conn



#Step 3: Request movie soundtrack data from Spotipy and store in soundtrack table
def fetch_soundtrack_data(cur, conn, token):
    
    """
    Fetches soundtracks for movies from the year 2024 stored in the Movies table.

    Parameters:
    - cur: Database cursor.
    - conn: Database connection.
    - token: Spotify API access token.

    Returns:
    - None
    """
    sp = spotipy.Spotify(auth=token)
    movie_total = 0
    max_inserts = 25 

    # Fetch movies from 2024
    cur.execute("""SELECT id, title 
                FROM Movies 
                WHERE year = 2024
                AND id NOT IN (SELECT movie_id FROM soundtracks)""")
    movies = cur.fetchall()

    if not movies:
        print("No movies from 2024 found in the database.")
        return

    print("Fetching soundtracks for movies from 2024...")

    for movie_id, movie_title in movies:
        if movie_total >= max_inserts:
            print(f"Reached the limit of {max_inserts} rows for this execution. Stopping fetch operation.")
            break
        try:
            # Search for albums on Spotify using the movie title
            results = sp.search(q=movie_title, type="album", limit=1)
            albums = results["albums"]["items"]

            if not albums:
                print(f"No soundtrack found for movie: {movie_title}")
                continue

            # Insert soundtrack into the 'soundtracks' table
            album = albums[0]
            soundtrack_name = album["name"]

            if movie_title.lower() not in soundtrack_name.lower():
                print(f"Album '{soundtrack_name}' does not match the movie title '{movie_title}'. Skipping.")
                continue

            #Retrieve Genre of Album from search

            album_id = album["id"]
            album_details = sp.album(album_id)
            genres = album_details.get("genres", [])
            genre = genres[0] if genres else "Unknown"

            tracks = sp.album_tracks(album_id)["items"]
            total_duration_ms = sum(track["duration_ms"] for track in tracks)  # Sum up track durations
            total_duration_hours = total_duration_ms // 3600000  # 1 hour = 3,600,000 ms
            remaining_minutes = (total_duration_ms % 3600000) // 60000  # Remaining minutes after hours
            remaining_seconds = (total_duration_ms % 60000) // 1000  # Remaining seconds after minutes

            # Format total duration as HH:MM:SS
            total_duration = f"{total_duration_hours:02d}:{remaining_minutes:02d}:{remaining_seconds:02d}"
            try:
            
                cur.execute("""
                    INSERT OR IGNORE INTO soundtracks (movie_title, movie_id, soundtrack_name, genre, total_duration)
                    VALUES (?, ?, ?, ?, ?)
                """, (movie_title, movie_id, soundtrack_name, genre, total_duration))
                conn.commit()
                print(f"Inserted soundtrack: {soundtrack_name} for movie: {movie_title}")
                movie_total +=1
            except Exception as e:
                print(f"Error inserting soundtrack for {movie_title}. Error: {e}")
        except Exception as e:
            print(f"Error fetching soundtrack for {movie_title}. Error: {e}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"TOTAL MOVIES INSERTED INTO SOUNDTRACKS TABLE: {movie_total}")
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

def fetch_soundtrack_songs_data(cur, conn, token): 
    """
    Fetches songs for each soundtrack in the soundtracks table 
    and inserts them into the soundtrack_songs table.

    Parameters:
    - cur: Database cursor.
    - conn: Database connection.
    - token: Spotify API token for authorization.

    Returns:
    - None
    """
    song_total = 0
    max_songs = 25
    sp = spotipy.Spotify(auth=token)

    # Fetch all soundtracks from the database
    cur.execute("""SELECT id, soundtrack_name 
                FROM soundtracks
                WHERE id NOT IN (SELECT soundtrack_id FROM soundtrack_songs)""")
    soundtracks = cur.fetchall()

    if not soundtracks:
        print("No soundtracks found in the database.")
        return
    for soundtrack_id, soundtrack_name in soundtracks:
        if song_total >= max_songs:
            print(f"Reached the limit of {song_total} rows for this execution. Stopping fetch operation.")
            return
        try:
            # Search for the album by name
            results = sp.search(q=soundtrack_name, type="album", limit=1)
            albums = results["albums"]["items"]

            if not albums:
                print(f"No album found for soundtrack: {soundtrack_name}")
                continue

            album_id = albums[0]["id"]

            # Get all songs (tracks) for the album
            tracks_data = sp.album_tracks(album_id)

            for track in tracks_data["items"]:
                if song_total >= max_songs:
                    print(f"Reached the limit of {song_total} rows for this execution. Stopping fetch operation.")
                    return
                song_title = track["name"]
                song_length_ms = track["duration_ms"]
                song_length_minutes = song_length_ms // 60000  # Convert to minutes
                song_length_seconds = (song_length_ms % 60000) // 1000  # Remainder in seconds
                song_length = f"00:{song_length_minutes:02d}:{song_length_seconds:02d}"

                # Insert the song into the soundtrack_songs table
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO soundtrack_songs (song_title, soundtrack_id, song_length)
                        VALUES (?, ?, ?)
                    """, (song_title, soundtrack_id, song_length))
                    print(f"Inserted song: {song_title} in soundtrack ID: {soundtrack_id}, length: {song_length}")
                    song_total += 1
                    conn.commit() 
                except Exception as e:
                    print(f"Error inserting song: {song_title}. Error: {e}")
        except Exception as e:
            print(f"Error fetching songs for soundtrack: {soundtrack_name}. Error: {e}")
#Step 5: Define main function
def main():
    db_name = "movies.db"
    token = get_token()

    # Set up database tables
    cur, conn = create_soundtrack_and_song_tables(db_name)

    # Fetch soundtracks only for 2024 movies
    fetch_soundtrack_data(cur, conn, token)

    # Fetch songs for each soundtrack
    fetch_soundtrack_songs_data(cur, conn, token)

    conn.close()
    print("\nSoundtrack data successfully fetched and stored!")
                                                                                                                                                            

# Step 6: Run the main function
if __name__ == "__main__":
   main()


