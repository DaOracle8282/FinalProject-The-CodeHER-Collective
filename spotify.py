import sqlite3
import spotipy
import spotipy.oauth2 as oauth2
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import sys
import webbrowser
from json.decoder import JSONDecodeError
import requests
import json
import os
import re


#Step 1: Set up connection to Spotipy
def get_token():
   CLIENT_ID = "cdc220444d2a42f5a7c4472fbe862667"
   CLIENT_SECRET = "7f72898d496246ea978f8886753a3557"

   sp_oauth = oauth2.SpotifyClientCredentials(client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET)
   token_info = sp_oauth.get_access_token()
   return token_info["access_token"]
 


#Step 2: Create soundtrack table in existing Movies database
def create_soundtrack_table(db_name):
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
   cur.execute("""CREATE TABLE IF NOT EXISTS Soundtracks ( 
               id INTEGER PRIMARY KEY,
               movie_title TEXT, 
               artists TEXT, 
               album_name TEXT, 
               genre TEXT
               )
                """)
   conn.commit()
   return cur, conn


#Step 3: Request movie soundtrack data from Spotipy and store in soundtrack table
def fetch_spotify_data(cur, conn, token, title):
    offset = 0
    sp = spotipy.Spotify(auth=token)
    try:
            # Search for albums with the keyword 'soundtrack'
        results = sp.search(q=title, type="album", limit=2, offset=offset)
        return results
    except Exception as e:
        print(f"Error inserting soundtrack: {title}. Error: {e}")

'''
   offset = 0
   limit = 50

    # Count existing soundtracks in the database
   cur.execute("SELECT COUNT(*) FROM Soundtracks")
   soundtracks_count = cur.fetchone()[0]

   if soundtracks_count >= limit:
        print(f"Database already contains {soundtracks_count} soundtracks. Limit reached.")
        return


   while soundtracks_count < limit:
        

            albums = results["albums"]["items"]

            if not albums:
                print("No more soundtracks found.")
                break

            for album in albums:
                album_name = album["name"]
                artists = ", ".join(artist["name"] for artist in album["artists"])
                genre = "Soundtrack"  # Assuming all are soundtracks

                # Insert into the database
                try:
                    cur.execute("""
                        INSERT OR IGNORE INTO Soundtracks ( movie_title, album_name, artists, genre)
                        VALUES (?, ?, ?)
                    """, (album_name, artists, genre))
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

'''

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
   cur, conn = create_soundtrack_table("movies.db")
    # Example movie titles
                                                                                                                                                                   

   soundtrack_query(cur)
   conn.close()
'''
# Step 6: Run the main function
if __name__ == "__main__":
   main()
'''
token = get_token()
cur, conn = create_soundtrack_table("movies.db")
movie = "Wicked"
wicked_results = fetch_spotify_data(cur, conn,token, movie)
print(wicked_results)
