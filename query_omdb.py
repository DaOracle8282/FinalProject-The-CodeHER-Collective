import sqlite3

def query_shared_movies_and_soundtracks(cur):
    """
    Queries movies that have corresponding soundtracks in the Soundtracks table.
    """
    cur.execute("""
        SELECT m.title, s.album_name
        FROM Movies m
        JOIN Soundtracks s ON m.title = s.album_name
        ORDER BY m.title ASC
    """)
    return cur.fetchall()

def calculate_average_rating(cur):
    """
    Calculates the average IMDb rating for all movies in the Movies table.
    """
    cur.execute("SELECT AVG(imdb_rating) FROM Movies")
    return cur.fetchone()[0]

def genre_count(cur):
    """
    Calculates the count of each genre in the Movies table.
    """
    cur.execute("""
        SELECT genre, COUNT(*) as count
        FROM Movies
        GROUP BY genre
        ORDER BY count DESC
    """)
    return cur.fetchall()


