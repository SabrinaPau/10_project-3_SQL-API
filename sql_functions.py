from dotenv import dotenv_values
import pandas as pd
import sqlalchemy
import psycopg2
import requests # package for getting data from the web
from zipfile import * # package for unzipping zip files
import os
import time

path ='../data/' 
cities = [
    "New York, NY",
    "Washington, DC",
    "Boston, MA",
    "Philadelphia, PA",
    "Miami, FL",
    "Houston, TX",
    "San Francisco, CA",
    "Seattle, WA"
]
cancellation_code = {
    "A": "mechanical issues",
    "B": "weather conditions",
    "C": "traffic control issue",
    "D": "personal reasons",
}
columns_to_keep = [
    "Year",
    'Month',
    'DayofMonth',
    'FlightDate',
    'DepDelay',
    'ArrDelay',
    'OriginCityName',
    'DestCityName',
    'Cancelled',
    'CancellationCode',
]
# Cities to get the data from:
stations = [
    '74486',            # New York - John F. Kennedy Airport
    '72405',            # Washington D.C. National Airport
    '72408',             # Philadelphia, PA
    '72202',            #'Miami International Airport'
    '72243',            #'Houston, TX Intercontinental'
    '72494',            #'San Francisco Airport'
    '72793'             # Seattle-Tacoma Airport
]


def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database','user','password']
    dotenv_dict = dotenv_values("../.env")
    sql_config = {key:dotenv_dict[key] for key in needed_keys if key in dotenv_dict}
    return sql_config


def get_data(query):
   ''' Connect to the PostgreSQL database server, run query and return data'''
    # get the connection configuration dictionary using the get_sql_config function
   sql_config = get_sql_config()

    # create a connection engine to the PostgreSQL server
   engine = sqlalchemy.create_engine('postgresql://user:pass@host/database',
                                     connect_args=sql_config # use dictionary with config details
                                     )
    
    # open a conn session using 'with', execute the query, and return the results
   with engine.begin() as conn: 
      results = conn.execute(query)
      # return pd.DataFrame(results.fetchall())
      return results.fetchall()


# Insert the get_dataframe() function definition below - do this only when instructed in the notebook

def get_dataframe(sql_query):
    ''' 
    Connect to the PostgreSQL database server, 
    run query and return data as a pandas dataframe
    '''
    engine = get_engine()
    return pd.read_sql_query(sql_query, engine)




def get_engine():
    sql_config = get_sql_config()
    engine = sqlalchemy.create_engine('postgresql://user:pass@host/database',
                        connect_args=sql_config
                        )
    return engine  



def download_data(year, month):
    ''' Get the file from the website https://transtats.bts.gov ''' 
    zip_file = f'On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip'
    url = (f'https://transtats.bts.gov/PREZIP/{zip_file}')
    ''' Download the database ''' 
    r = requests.get(f'{url}', verify=False)
    ''' Save database to local file storage''' 
    with open(path+zip_file, 'wb') as f:
        f.write(r.content)
        print(f'--> zip_file with name: {zip_file} downloaded succesfully.' )



def extract_zip(year, month):
    ''' Get the file from the website https://transtats.bts.gov '''
    zip_file = f'On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip'
    with ZipFile(path+zip_file, 'r') as zip_ref:
        zip_ref.extractall(path)
        csv_file =  zip_ref.namelist()[0]
        print(f'--> zip_file was succesfully extracted to: {csv_file}.' )


def create_table(year, months_list):

    ''' Check if the data folder already exist, if not create it. '''
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"--> Directory created: {path}")
    else:
        print(f"--> Directory already exists: {path}")

    ''' downloading the data as a zip from the url and extracting the zip file. '''
    for month in months_list:
        download_data(year, month)
        extract_zip(year, month)

    ''' reading each file for the specific year and appending to a list. '''
    flights_data = []
    for month in months_list:
        file_path = f"../data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.csv"
        try:
            flights_data.append(pd.read_csv(file_path))
        except FileNotFoundError:
            print(f"File not found for {month}")

    ''' concatenate all files into a single df, so we have all the months of the
    specific year together in a df. '''
    flights_concat = pd.concat(flights_data)

    ''' filter the columns we want to keep and create a copy of the original df. '''
    flights_columns = flights_concat[columns_to_keep].copy()

    ''' make all the columns lower case and rename some columns to have a better
    understanding name. '''
    flights_columns.columns = flights_columns.columns.str.lower()
    flights_columns.rename(
	columns={
		'dayofmonth': 'day',
		'flightdate': 'flight_date',
		'depdelay': 'dep_delay',
		'arrdelay': 'arr_delay',
		'origincityname': 'origin',
		'destcityname': 'destination',
		'cancellationcode': 'cancellation_code',
	}, inplace=True)

    ''' Filter the only the cities we want to have. In these case we want to have the cities
    from the the list 'cities' being either the origin of the flight or the destination of the flight. '''
    flights = flights_columns[flights_columns["origin"].isin(cities) | flights_columns["destination"].isin(cities)]


    flights['cancellation_code_info'] = flights['cancellation_code'].map(cancellation_code)
    flights["cancellation_code_info"] = flights["cancellation_code_info"].fillna("not cancelled")
    flights.drop_duplicates(inplace=True)
    flights["had_delay"] = ((flights["arr_delay"] < 0) | (flights["dep_delay"] < 0)).astype(int)
    flights['flight_date'] = pd.to_datetime(flights['flight_date'])
    flights['cancelled']  = flights['cancelled'].astype(int)
    return flights

def push_to_cloud(table, name):
    schema = 'group3'
    table_name = name

    engine = get_engine()

    if engine!=None:
        try:
            table.to_sql(name=table_name, # Name of SQL table variable
                            con=engine, # Engine or connection
                            schema=schema, # your class schema variable
                            if_exists='replace', # Drop the table before inserting new values 
                            index=False, # Write DataFrame index as a column
                            chunksize=5000, # Specify the number of rows in each batch to be written at a time
                            method='multi') # Pass multiple values in a single INSERT clause
            print(f"The {table_name} table was imported successfully.")
        # Error handling
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            engine = None
        else:
            print('No engine')


def get_weather_data(start_dates, end_dates):
    # Set the url
    url = "https://meteostat.p.rapidapi.com/stations/daily"

    headers = {
        "X-RapidAPI-Key": os.getenv('api_key'),
        "X-RapidAPI-Host": "meteostat.p.rapidapi.com"
    }

    # Create empty dataframe, will be used to append each location's weather data
    weather_df = pd.DataFrame([])

    # Loop through all locations and all dates
    for station in stations:
        for start_date, end_date in zip(start_dates, end_dates):
            querystring = {
                            "station": station,
                            "start": start_date,
                            "end": end_date,
                            "units": "metric"
                            }
            
            # Request data from url
            r = requests.get(url, headers=headers, params=querystring)
            time.sleep(5) #uncomment if you run into a query limit
            # Decode response with json decoder
            weather_temp = r.json()

            # Flatten json response
            weather_temp_df = pd.json_normalize(weather_temp,
                                                sep='_',
                                                record_path='data',
                                                record_prefix='weather_'
                                                )
            
            # Set the station code for all rows related to the current date range
            weather_temp_df['station'] = station

            # Concatenate dataframes
            weather_df = pd.concat([weather_df, weather_temp_df], ignore_index=True)

    # Print final dataset weather_df
    return weather_df


def clean_weather_data(weather):
    weather.drop(['weather_wdir', 'weather_wpgt', 'weather_pres', 'weather_tsun'], axis=1, inplace=True)
    weather['weather_date'] = pd.to_datetime(weather['weather_date'])
    weather.rename(columns={
        'weather_date': 'date',
        'weather_tavg': 'avg_temp_°C',
        'weather_tmin': 'min_temp_°C',
        'weather_tmax': 'max_temp_°C',
        'weather_prcp': 'preciptation_mm',
        'weather_snow': 'snowdepth_mm',
        'weather_wspd': 'avg_windspeed_kmh'
    }, inplace=True)

    station_names = {
        '74486': 'New York, NY',	
        '72405': 'Washington, DC',
        '72408': 'Philadelphia, PA',
        '72202': 'Miami, FL',
        '72243': 'Houston, TX',
        '72494': 'San Francisco',
        '72793': 'Seattle, WA'
    }
    weather['city_name'] = weather['station'].map(station_names)
    weather.drop(['station'], axis=1, inplace=True)
    weather['year'] = weather['date'].dt.year
    weather['month'] = weather['date'].dt.month
    weather['day'] = weather['date'].dt.day
    return weather
