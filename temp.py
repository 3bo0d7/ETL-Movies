
from datetime import datetime, timedelta
import tmdbsimple as tmdb
import json

TMDB_API_KEY = '3098581a010a1964a2a57c115c3eb5a2'
tmdb.API_KEY = TMDB_API_KEY

# scrape the top 1000 movies
def scrape_top_movies():
    movie_data = []
    total_movies = 1000
    movies_per_page = 20
    total_pages = total_movies // movies_per_page

    for page in range(1, total_pages + 1):
        movies = tmdb.Movies().popular(page=page)
        movie_data.extend(movies['results'])

    # Save movie data to JSON / fix path!
    with open('/Users/AbdAhmed3/Library/CloudStorage/OneDrive-MAJIDALFUTTAIMPROPERTIESLLC/Desktop/GitHub/ETL-Movies/Top1000.json', 'w') as f:
        json.dump(movie_data, f,indent= 2)


if __name__ == "__main__" :
    scrape_top_movies()