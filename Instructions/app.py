# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %%
get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# %%
import numpy as np
import pandas as pd


# %%
import datetime as dt

# %% [markdown]
# # Reflect Tables into SQLAlchemy ORM

# %%
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


# %%
engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# %%
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# %%
# We can view all of the classes that automap found
Base.classes.keys()


# %%
# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station


# %%
# Create our session (link) from Python to the DB
session = Session(engine)

# %% [markdown]
# # Exploratory Climate Analysis

# %%
# Calculate the date 1 year ago from the last data point in the database
last_twelve = dt.date(2017,8,23)-dt.timedelta(days=365)
last_twelve 


# %%
# Design a query to retrieve the last 12 months of precipitation data and plot the results
precip_data = session.query(measurement.date, measurement.prcp).filter(measurement.date>=last_twelve).all()
precip_data


# %%
# Save the query results as a Pandas DataFrame and set the index to the date column
measurement_df = pd.DataFrame(precip_data) 
#measurement_df = measurement_df.rename(columns={"date": "Date","prcp": "Precipitation"})
measurement_df.set_index(measurement_df["date"], inplace=True)
del measurement_df["date"]
measurement_df


# %%
# Sort the dataframe by date
measurement_df = measurement_df.sort_values("date")
measurement_df


# %%
# Use Pandas Plotting with Matplotlib to plot the data
precip_plot = measurement_df.plot(figsize=(20,10), color='blue', rot=45)

plt.xlabel("Date", size=20)
plt.ylabel("Precipitation", size=20)
plt.title("Last 12 Months of Precipitation", size=20)
plt.tight_layout()
plt.savefig("Last_12_Months_of_Precip.png")


# %%
# Use Pandas to calculate the summary statistics for the precipitation data
measurement_df.describe()


# %%
# Design a query to show how many stations are available in this dataset?
session.query(func.count(station.station)).all()


# %%
# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.
session.query(measurement.station, func.count(measurement.station)).group_by(measurement.station).order_by(func.count(measurement.station).desc()).all()


# %%
# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature of the most active station?
session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).filter(measurement.station=="USC00519281").all()


# %%
# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station 
station_high = session.query(measurement.tobs).filter(measurement.station == "USC00519281").filter(measurement.date>=last_twelve).all()
station_high


# %%
station_high = pd.DataFrame(station_high)
station_high


# %%
# plot the results as a histogram
station_high.plot.hist(bins=12)
plt.title("Last 12 Months of Temperature")
plt.xlabel("Temperature")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig("Last_12_Months_of_Temp")

# %% [markdown]
# ## Bonus Challenge Assignment

# %%
# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))


# %%
# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.


# %%
# Plot the results from your previous query as a bar chart. 
# Use "Trip Avg Temp" as your Title
# Use the average temperature for the y value
# Use the peak-to-peak (tmax-tmin) value as the y error bar (yerr)


# %%
# Calculate the total amount of rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation


# %%
# Create a query that will calculate the daily normals 
# (i.e. the averages for tmin, tmax, and tavg for all historic data matching a specific month and day)

def daily_normals(date):
    """Daily Normals.
    
    Args:
        date (str): A date string in the format '%m-%d'
        
    Returns:
        A list of tuples containing the daily normals, tmin, tavg, and tmax
    
    """
    
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
    
daily_normals("01-01")


# %%
# calculate the daily normals for your trip
# push each tuple of calculations into a list called `normals`

# Set the start and end date of the trip

# Use the start and end date to create a range of dates

# Stip off the year and save a list of %m-%d strings

# Loop through the list of %m-%d strings and calculate the normals for each date


# %%
# Load the previous query results into a Pandas DataFrame and add the `trip_dates` range as the `date` index


# %%
# Plot the daily normals as an area plot with `stacked=False`

