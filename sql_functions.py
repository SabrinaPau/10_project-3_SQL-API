# We import a method from the  modules to address environment variables and 
# we use that method in a function that will return the variables we need from .env 
# to a dictionary we call sql_config

from dotenv import dotenv_values

def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database','user','password']
    dotenv_dict = dotenv_values("../.env")
    sql_config = {key:dotenv_dict[key] for key in needed_keys if key in dotenv_dict}
    return sql_config

# Import sqlalchemy and pandas - do this only when instructed

import pandas as pd
import sqlalchemy 

# Insert the get_data() function definition below - do this only when instructed in the notebook

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

# Insert the get_engine() function definition below - when instructed

def get_engine():
    sql_config = get_sql_config()
    engine = sqlalchemy.create_engine('postgresql://user:pass@host/database',
                        connect_args=sql_config
                        )
    return engine  

cities = [
    "New York, NY",
    "Boston, MA",
    "Washington, DC",
    "Newark, NJ",
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

def create_table(year):
    flights_dec = pd.read_csv(f'./data/FLIGHTS_REPORTING_{year}_DEC.csv')
    flights_jan = pd.read_csv(f'./data/FLIGHTS_REPORTING_{year}_JAN.csv')
    flights_feb = pd.read_csv(f'./data/FLIGHTS_REPORTING_{year}_FEB.csv')

    flights = pd.concat([flights_dec, flights_jan, flights_feb])

    origin = flights[flights["ORIGIN_CITY_NAME"].isin(cities)] 

    destination = flights[flights["DEST_CITY_NAME"].isin(cities)]

    flights_clean = pd.concat([origin, destination])

    flights_clean.drop(columns=["YEAR", "MONTH", "DAY_OF_MONTH", "ORIGIN", "DEST", "DEP_TIME", "ARR_TIME"], inplace=True)

    flights_clean.columns = flights_clean.columns.str.lower()
    flights_clean['cancellation_code_info'] = flights_clean['cancellation_code'].map(cancellation_code)
    flights_clean["cancellation_code_info"] = flights_clean["cancellation_code_info"].fillna("not cancelled")

    return flights_clean

import psycopg2

def push_to_cloud(table, year):
    schema = 'group3'
    table_name = f'flights_{year}'

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