import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import sys
import spotipy.util as util
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
   REDIRECT_URI = "https://www.google.com/?code=AQBvXoNUMhosHVyEwejSFYk1sI6kyUR0bbJIT0N1XCtSrF5nKqOQxf7yf07ZI4-QTMTT82ri_RIUfiGt9pynrx-dBGj7lHgnhJoe6WduFFzSKPF_ehjXrHHvUy4pKcd_IpnyJkEpEZ6agYHtw6yxap6rghCTaP0QUIFhjogQvH4R8y1dcrVUNAM"

   sp_oauth = SpotifyOAuth(client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET,
                            redirect_uri=REDIRECT_URI,
                            scope="user-library-read")
   token_info = sp_oauth.get_cached_token()
   return token_info['access_token']


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
   cur.execute("DROP TABLE IF EXISTS Soundtracks")
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
   sp = spotipy.Spotify(auth=token)
   try:
         results = sp.search(q=title, type="album", limit=1)
         album = results["albums"]["items"][0]
         album_name = album["name"]
         artists = ", ".join(artist["name"] for artist in album["artists"])
         genre = "Soundtrack"
         cur.execute("""
            INSERT OR IGNORE INTO Soundtracks (movie_title, artists, album_name, genre)
            VALUES (?, ?, ?, ?)
            """, (title, artists, album_name, genre))
   except Exception as e:
         print(f"Error fetching data for {title}: {e}")

   conn.commit()



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
   movie_titles = ["Inception", "Avatar", "Interstellar", "The Dark Knight"]
   for title in movie_titles:
      fetch_spotify_data(cur, conn, token, title)

   soundtrack_query(cur)
   conn.close()

# Step 6: Run the main function
if __name__ == "__main__":
   main()
