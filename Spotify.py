import sqlite3
import spotipy
import sys
import spotipy.util as util
import webbrowser
from json.decoder import JSONDecodeError
import requests 
import json
import unittest
import os
import re

#Step 1: Set up connection to Spotipy
def get_token():
   #Get username from terminal
   username =sys.argv[1]
   # User ID: 31jrnyjlbu5eu3ghppc5g25fafty
   

   #Erase cache and prompt for user permission
   try:
      token =util.prompt_for_user_token(username)
   except:
      os.remove(f".cache-{username}")
      token = util.prompt_for_user_token(username)


   #create Spotipy object
   sp = spotipy.Spotify(auth=token)

   return token
   

#print(json.dumps(VARIABLE, sort_keys = True, indent = 4))

#Step 2: Create soundtrack table in existing Movies database
def create_soundtack_table(db_name):
    """
   creates the soundtracktable.

    Parameters:
    - db_name (str): Name of the SQLite database file.

    Returns:
    - cursor, connection: Database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(os.path.join(path, db_name))
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS Soundtracks id INTEGER PRIMARY KEY, 
                movie title TEXT, artist(s) TEXT, album name TEXT, genre TEXT 
                 """)
    conn.commit()
    return cur, conn

#Step 3: Request movie soundtrack data from Spotipy and store in soundtrack table





#Step 4: Run a query on Soundtracks table




#Step 5: Define main function 
def main():
   token = get_token()
   create_soundtack_table("movies.db")



# Step 6: Run the Main Function
if __name__ == "__main__":
    main()