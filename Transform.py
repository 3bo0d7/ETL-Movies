import pandas as pd
from pandas import read_csv
import numpy as np 
import csv
from tabulate import tabulate


randomdatamovies = 'RandomData-ETL-Movies.csv'
df = read_csv(randomdatamovies)
df1 = pd.DataFrame(df)

# print(tabulate(df, headers = 'keys', tablefmt = 'psql'))

df1['Title'] = df1['Title'].str.strip()
df1['Release Date'] = pd.to_datetime(df1['Release Date'], errors='coerce', dayfirst=True)  # Convert to datetime
df1['Runtime'] = pd.to_numeric(df['Runtime'], errors='coerce')  # Convert runtime to numeric
df1['Runtime'].fillna(df1['Runtime'].mean(), inplace=True)
df['Director'].fillna('Unknown', inplace=True)

print(df1)
