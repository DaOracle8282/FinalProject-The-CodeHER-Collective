
import pandas
import os
import sqlite3
import requests
import matplotlib.pyplot as plt




def fetch_movie_data(movie_titles):
    """
    Fetches data from the OMDb API for a list of movie titles.

    Parameters:
    - movie_titles (list): List of movie titles to fetch data for.

    Returns:
    - list: List of movie data dictionaries.
    """
    base_url = "http://www.omdbapi.com/"
    api_key = "25781136"  # Your specific API key
    movies_data = []

    for title in movie_titles:
        # Make API request with the provided API key
        response = requests.get(base_url, params={"t": title, "apikey": api_key})
        if response.status_code == 200:
            data = response.json()
            if data.get("Response") == "True":
                movies_data.append(data)  # Add movie data to the list
            else:
                print(f"Movie not found: {title}")  # Handle missing movies
        else:
            print(f"Error fetching data for {title}: {response.status_code}")  # Handle API errors

    return movies_data




