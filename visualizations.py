import sqlite3
import matplotlib.pyplot as plt

def calculate_aggregate_by_table(cur, lookup_table, fk_column, aggregate="COUNT(*)"):
    """
    General function to calculate aggregates for movies using a lookup table.
    """
    query = f"""
        SELECT {lookup_table[:-1]} AS name, {aggregate}
        FROM Movies
        JOIN {lookup_table} ON Movies.{fk_column} = {lookup_table}.id
        GROUP BY {lookup_table}.id
    """
    cur.execute(query)
    return {row[0]: row[1] for row in cur.fetchall()}

def plot_bar_chart(data, title, xlabel, ylabel):
    """
    Plots a bar chart with custom colors, bold titles, and data annotations.
    """
    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 6))
    
    # Use a color palette
    colors = plt.cm.coolwarm(range(len(data)))  # Color map with distinct colors
    
    # Create the bar chart
    bars = plt.bar(data.keys(), data.values(), color=colors)
    
    # Add bold title and labels
    plt.title(title, fontsize=14, fontweight='bold')  # Bold title
    plt.xlabel(xlabel, fontsize=12, fontweight='bold')  # Bold x-axis label
    plt.ylabel(ylabel, fontsize=12, fontweight='bold')  # Bold y-axis label
    
    # Rotate x-ticks for better readability
    plt.xticks(rotation=45, ha="right", fontsize=10)
    
    # Annotate bars with values
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, f'{yval}', 
                 ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Add a tight layout to improve spacing
    plt.tight_layout()
    plt.show()


def main():
    """
    Main function for generating visualizations.
    """
    db_name = "movies2024.db"
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Total movies by genre
    print("Visualizing total movies by genre...")
    genre_counts = calculate_aggregate_by_table(cur, "Genres", "genre_id")
    plot_bar_chart(genre_counts, "Total Movies by Genre", "Genre", "Number of Movies")

    # Total movies by country
    print("Visualizing total movies by country...")
    country_counts = calculate_aggregate_by_table(cur, "Places", "place_id")
    plot_bar_chart(country_counts, "Total Movies by Country", "Country", "Number of Movies")

    # Average ratings by genre
    print("Visualizing average IMDb ratings by genre...")
    avg_genre_ratings = calculate_aggregate_by_table(cur, "Genres", "genre_id", "ROUND(AVG(Movies.imdb_rating), 2)")
    plot_bar_chart(avg_genre_ratings, "Average IMDb Ratings by Genre", "Genre", "Average Rating")

    # Average ratings by country
    print("Visualizing average IMDb ratings by country...")
    avg_country_ratings = calculate_aggregate_by_table(cur, "Places", "place_id", "ROUND(AVG(Movies.imdb_rating), 2)")
    plot_bar_chart(avg_country_ratings, "Average IMDb Ratings by Country", "Country", "Average Rating")

    conn.close()

if __name__ == "__main__":
    main()
