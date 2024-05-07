# SQL-API project

It's project time again :tada:!  
In this project you will combine your SQL, Python and API skills and use them in combination with each other.  

## Objective
As we have learned, the two main tools of Data Analysts are SQL and Python.  
In the last lectures and exercises, you have learned how to use SQL, how to get data out of a database into a pandas dataframe and how to enrich your data with the help of APIs.
And now you should do it altogether.


## Task Overview
The Research Center for Aerospace (RCA) where you work for as Data Analyst wants to keep track of accumulated flights data in combination with weather data. Your task is to find a situation where the weather has impacted on flight performance and use this to contribute some knowledge about how different weather affects flights in different cities.

![](images/PIREPs-featured.jpg)  
### Setting-up working environment

1. Fork this repository in Github - but: team members work best on one repository collaboratively. 
2. Open VS Code and a terminal window in VS Code
3. It's up to you: **Use your existing nf_sql environment** from our exercises __or(!)__ create a new conda environment for this project. In both cases you can install any additional packages needed for doing some EDA and visualizations.  

```ZSH 
# Create new environment, eg.:
conda create --clone nf_sql --name <your_env_name>
# Activate environment 
$ conda activate <your_env_name>
# Installing packages 
$ conda install <package_name>
```
Activate your environment.  

4. Add your sql_functions.py file (and maybe your .env) from the external data sourcing notebooks:
```zsh
$ cp ../da-external-data-sourcing/.env .
$ cp ../da-external-data-sourcing/sql_functions.py .
```
5. Check your .gitignore in order to avoid pushing credentials to GitHub.  
6. Open one or more new notebook(s) in VS Code to work in.  

### Task steps
1. Choose a historical weather event that occured in the United States sometime in the past 30 years that you believe would have led to the cancellation of flights.

2. Get data on flights & Set up a connection to our SQL database.  
  a. As described in [this notebook](https://github.com/neuefische/da-sql-api-project/blob/main/get_flights_data.ipynb), download csv file containing flights data for the specific years and months you need from the [Bureau of Transportation Statistics website](https://transtats.bts.gov).    
  b. Clean your data (e.g. specify which columns you want to keep, rename columns etc.).  
  c. Reduce your dataframe to include at least 5 origin airports (choose either big cities or locations from this [list of locations with weather stations](https://bulk.meteostat.net/v2/stations/lite.json.gz)). You can expand your dataset to include more locations or destinations if this is helpful in your analysis.   
  d. Make an EDA on the flights data you have downloaded to explain what data you have and any unexpected findings.   
  e. Connect to database and join the data with the airports table of our database to get the latitude, longitude or city names for the airports in your dataset. 
  
3. As next step, get historical weather data using the [Meteostat API](https://dev.meteostat.net/api/point/daily.html#endpoint).   
  a. Follow the steps [here](https://dev.meteostat.net/api/) and sign-up to RapidAPI in ordet to get access to the Meteostat API  
  b. Read the docs to find out what the call limits are for this API. Ensure your data retrieval needs (including testing) fit within these limits.  
  c. Use your API key to get weather data for your chosen month/year and locations.  
  d. If necessary flatten your JSON data and transform it into a DataFrame for future analysis.  
  e. Make sure to have primary and foreign keys so that it's possible to join the weather data to your flights data.  
  
4. Perform a basic EDA on both of the tables.  
  a. Come up with three different hypotheses regarding your available data, taking into account both of the datasets you have. You should start with asking these questions: "Can we see the weather event in the weather data?", "Can we see the weather event in the flights data?", "Can we see a correllation between the data".  
  b. Go deeper into your hypotheses (perhaps linking dep_delay to weather) and clearly outline your findings (either that everything is as expected or any unexpected results).  

### Deliverables
1. Clean and structured .ipynb notebook containing the (well-documented) code to connect to database as well as API as well as the required EDA part.
2. ~10-minutes technical presentation (eg. via google slides) to your colleagues, presenting the results of your data exploration and answering your hypotheses.


**And:**  
**Keep in mind that your API calls are limited!**  
**When possible, separate code calling the API from other code working on the data.**
