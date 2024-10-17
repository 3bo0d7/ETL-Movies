import pandas as pd
from pandas import *
import numpy as np 
import csv
from tabulate import tabulate

df = read_csv('RandomData-ETL-Movies.csv')
print(tabulate(df, headers = 'keys', tablefmt = 'psql'))

