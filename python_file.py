#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 13 16:26:24 2023

@author: jencyroberts
"""

#pip install sqlalchemy

import requests
import pandas as pd
from sqlalchemy import create_engine
#from sqlalchemy_utils import database_exists, create_database

# API keys
weather_api_key = '43b8d27b1bdcade58fa9bf69f3c6f276'
news_api_key = 'f82c206b3f894b14bc4cb39b8d0386cb'

# API endpoints
weather_url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}'
news_url = 'https://newsapi.org/v2/top-headlines?country={}&apiKey={}'

# Defining the database connection parameters
db_username = 'db_username'
db_password = 'db_password'
db_name = 'db_name'
db_host = 'db_host'
db_port = 'db_port'

# Define function to fetch weather data
def get_weather_data(city):
    url = weather_url.format(city, weather_api_key)
    response = requests.get(url)
    data = response.json()
    return data

# Define function to fetch news data
def get_news_data(country):
    url = news_url.format(country, news_api_key)
    response = requests.get(url)
    data = response.json()
    return data

# Define function to transform and load data into database
def transform_and_load_data(data):
    # Transform data into a pandas DataFrame
    df = pd.DataFrame(data)
    
    
    #DataFrame into database using SQLAlchemy
    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    df.to_sql('data_table', engine, if_exists='append', index=False)
    engine.dispose()
    
    print(df)
    
# Defining function to transform and load data into database
def transform_and_load_data(data):
    # Transform data into a pandas DataFrame
    if 'articles' in data:
        df = pd.DataFrame(data['articles'])
    else:
        df = pd.DataFrame({
            'city': data.get('name'),
            'country': data['sys'].get('country'),
            'weather_description': data['weather'][0]['description'],
            'temperature': data['main'].get('temp'),
            'pressure': data['main'].get('pressure'),
            'humidity': data['main'].get('humidity')
        }, index=[0])
    
    # Load DataFrame into database using SQLAlchemy
    engine = create_engine(f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    df.to_sql('data_table', engine, if_exists='append', index=False)
    engine.dispose()

# Define main function to run the pipeline
def main():
    # Define list of cities and countries to fetch data for
    cities = ['Oslo', 'New York', 'Tokyo', 'London']
    countries = ['no','us', 'jp', 'gb']
    
    # Fetch weather data for each city and load into database
    for city in cities:
        data = get_weather_data(city)
        transform_and_load_data(data)
    
    # Fetch news data for each country and load into database
    for country in countries:
        data = get_news_data(country)
        transform_and_load_data(data)


# Call main function to run the pipeline
if name == 'main':
    main()