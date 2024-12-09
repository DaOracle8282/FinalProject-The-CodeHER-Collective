import sqlite3
import spotipy
import spotipy.oauth2 as oauth2
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import sys
from json.decoder import JSONDecodeError
import os


'''
Things to still work out (Complete ): 
-filter albums by year when pulling from api to match albums with movies (this could possibly be a join to match together movies with soundtrack)
-ensure fetch_spotify data is pulling 25 or less items each time it is run for a total of 100 or more
-what is going to be calculated from this api's tables? maybe average length of albums, or top 5 longest albums, or top 5 most listened to songs? 

'''

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
               album_name TEXT UNIQUE, 
               genre TEXT,
               FOREIGN KEY (movie_id) REFERENCES movie(id)
               )
                """)
   
   cur.execute("""CREATE TABLE IF NOT EXISTS soundtrack_songs (
               id INTEGER PRIMARY KEY,
               song_title TEXT,
               st_id INTEGER,
               FOREIGN KEY (st_id) REFERENCES soundtracks(id))
               """)
   conn.commit()
   return cur, conn



#Step 3: Request movie soundtrack data from Spotipy and store in soundtrack table
def fetch_soundtrack_data(cur, conn, token, title):
    offset = 0
    limit = 100

    # Count existing soundtracks in the database
    cur.execute("SELECT COUNT(*) FROM Soundtracks")
    soundtracks_count = cur.fetchone()[0]

    if soundtracks_count >= limit:
        print(f"Database already contains {soundtracks_count} soundtracks. Limit reached.")
        return

    while soundtracks_count < limit:
        sp = spotipy.Spotify(auth=token)
        try:
            # Search for albums by title
            results = sp.search(q=title+ " year:2023", type="album", limit=25, offset=offset)
            albums = results["albums"]["items"]

            if not albums:
                print("No more soundtracks found.")
                break

            for album in albums:
                movie_title =title
                album_name = album["name"]
                artists = ", ".join(artist["name"] for artist in album["artists"])
                genre = "Soundtrack"  # Assuming all are soundtracks

                # Insert into the database
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO Soundtracks (movie_title, album_name, artists, genre)
                        VALUES (?,?, ?, ?)
                    """, (movie_title,album_name, artists, genre))
                    soundtracks_count += 1
                    print(f"Inserted: {album_name}")

                    if soundtracks_count >= limit:
                        break
                except Exception as e:
                    print(f"Error inserting soundtrack: {album_name}. Error: {e}")
            
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
    cur, conn = create_soundtrack_and_song_tables("movies.db")
    # Example movie titles
    conn.close()
                                                                                                                                                              
'''
   soundtrack_query(cur)
'''

# Step 6: Run the main function
if __name__ == "__main__":
   main()


