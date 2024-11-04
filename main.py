from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import tmdbsimple as tmdb
import json
import vertica_python

TMDB_API_KEY = '3098581a010a1964a2a57c115c3eb5a2'

VERTICA_CONN_INFO = {
    'host': 'abdallaalhalami-vertica.coder.svc',
    'port': 5433,
    'user': 'dbadmin',
    # 'password': '',
    # 'database': '',
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transform_top_1000_movies_vertica',
    default_args=default_args,
    description='Scrape and transform top 1000 movies, then load into Vertica DB',
    schedule_interval=timedelta(days=7),  # Runs weekly
)

# Initialize the TMDB API client with the API key
tmdb.API_KEY = TMDB_API_KEY


# Function to scrape the top 1000 movies from TMDB
def scrape_top_movies():
    movie_data = []
    total_movies = 1000
    movies_per_page = 20
    total_pages = total_movies // movies_per_page

    for page in range(1, total_pages + 1):
        movies = tmdb.Movies().popular(page=page)
        movie_data.extend(movies['results'])

    # Save the movie data to a JSON file for processing in the next step
    with open('/Users/AbdAhmed3/Library/CloudStorage/OneDrive-MAJIDALFUTTAIMPROPERTIESLLC/Desktop/GitHub/ETL-Movies/Top1000.json', 'w') as f:
        json.dump(movie_data, f)


def transform_and_load_data():
    with open('/path/to/save/top_1000_movies.json', 'r') as f:
        movie_data = json.load(f)

    # Clean and transform the data
    cleaned_data = []
    for movie in movie_data:
        cleaned_movie = {
            'id': movie.get('id'),
            'title': movie.get('title'),
            'release_date': movie.get('release_date'),
            'vote_average': movie.get('vote_average'),
            'vote_count': movie.get('vote_count'),
            'popularity': movie.get('popularity'),
            'overview': movie.get('overview'),
        }
        cleaned_data.append(cleaned_movie)

    with vertica_python.connect(**VERTICA_CONN_INFO) as connection:
        cursor = connection.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY,
                title VARCHAR,
                release_date DATE,
                vote_average FLOAT,
                vote_count INTEGER,
                popularity FLOAT,
                overview VARCHAR
            );
        ''')

        insert_query = '''
            INSERT INTO movies (id, title, release_date, vote_average, vote_count, popularity, overview)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        '''

        for movie in cleaned_data:
            cursor.execute(insert_query, (
                movie['id'],
                movie['title'],
                movie['release_date'],
                movie['vote_average'],
                movie['vote_count'],
                movie['popularity'],
                movie['overview'],
            ))


scrape_movies_task = PythonOperator(
    task_id='scrape_top_1000_movies',
    python_callable=scrape_top_movies,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load_movies_vertica',
    python_callable=transform_and_load_data,
    dag=dag,
)

# task dependencies for AirFlow , comment if python vvv
scrape_movies_task >> transform_and_load_task
