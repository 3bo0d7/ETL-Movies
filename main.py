# # Define the task that fetches the API data
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import tmdbsimple as tmdb
# import json
#
# TMDB_API_KEY = 'https://api.themoviedb.org/3/movie/550?api_key=3098581a010a1964a2a57c115c3eb5a2'
#
# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 10, 10),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#
# }
#
# # Define the DAG
# dag = DAG(
#     'scrape_top_1000_movies',
#     default_args=default_args,
#     description='Scrape the top 1000 movies from TMDB using tmdbsimple library',
#     schedule_interval=timedelta(days=7),
# )
#
# tmdb.API_KEY = TMDB_API_KEY
# # scrape the top 1000 movies
# def scrape_top_movies():
#     movie_data = []
#     total_movies = 1000
#     movies_per_page = 20
#     total_pages = total_movies // movies_per_page
#
#     for page in range(1, total_pages + 1):
#         movies = tmdb.Movies().popular(page=page)
#         movie_data.extend(movies['results'])
#
#     # Save movie data to JSON / fix path!
#     with open('/path/to/save/top_1000_movies.json', 'w') as f:
#         json.dump(movie_data, f)
#
# scrape_movies_task = PythonOperator(
#     task_id='scrape_top_1000_movies',
#     python_callable=scrape_top_movies,
#     dag=dag,
# )
#
#
#
