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
