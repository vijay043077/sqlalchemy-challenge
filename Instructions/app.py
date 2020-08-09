import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
import datetime as dt 

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()

Base.prepare(engine, reflect=True)

Station = Base.classes.station

measurement = Base.classes.measurement

app = Flask(__name__)

@app.route("/")
def Home_Page():
    """List of all available API routes."""
    return(
        f"Available API routes<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start><end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session= Session(engine)

    results = session.query(measurement.date, measurement.prcp).all()
    
    session.close()

    precipitation_data = []
    for date, prcp in results:
        precipitation_dict = {}
        precipitation_dict ["Date"] = date
        precipitation_dict ["Precipitation"] = prcp
        precipitation_data.append(precipitation_dict)

    return jsonify(precipitation_data)

@app.route("/api/v1.0/stations")
def station():
    
    session = Session(engine)

    results = session.query(Station.name).all()

    session.close()

    all_station = list(np.ravel(results))

    return jsonify(all_station)

@app.route("/api/v1.0/tobs")
def tobs():

    session = Session(engine)

    last_twelve_months = dt.date(2017,8,23)-dt.timedelta(days=365)
    
    results = session.query(measurement.date, measurement.prcp).filter(measurement.date>=last_twelve_months).all()

    session.close()

    temperature_data = []
    for date, tobs in results:
        temp_dict = {}
        temp_dict["Date"] = date
        temp_dict["Temperature"]= tobs
        temperature_data.append(temp_dict)

    return jsonify(temperature_data)

@app.route("/api/v1.0/<start>")
def start_date(start):

    session = Session(engine)
    
    start_date = dt.strptime(start,'%Y-%m-%d')
    
    results = session.query(func.min(measurement.tobs),func.max(measurement.tobs),func.avg(measurement.tobs)).\
        filter(measurement.date >=start_date).all()

    session.close()

    starting_date = list(np.ravel(results))

    return jsonify(starting_date) 
    
@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start,end):

    session = Session(engine)
    
    start_date = dt.strptime(start,'%Y-%m-%d')
    end_date = dt.strptime(end,'%Y-%m-%d')
   
    results = session.query(func.min(measurement.tobs),func.max(measurement.tobs),func.avg(measurement.tobs)).\
        filter(measurement.date >=start_date).filter(measurement.date<=end_date).all()

    session.close()

    startend_date = list(np.ravel(results))

    return jsonify(startend_date) 

if __name__ == '__main__':
    app.run(debug=True)