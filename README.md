Environment Canada Weather History
==================================
Command line project for parsing hourly historical weather data from 
Environment Canada given an appropriate stationID. Data can be returned via 
a CSV or inserted into a MySQL database.

If you only need a month of historical weather, consider using Environment 
Canada's default ?format=csv option. However, in addition to its SQL 
capabilities, this script can stitch all observations in a date range together 
into a single CSV file.

Currently this project is dependent on the MySQL connector being installed 
in the Python environment, but my goal is to quickly decouple that dependency.


Dependencies
============
Download and install the [MySQL Connector](https://dev.mysql.com/downloads/connector/python/).

Also depends on the pytz module, installed using:

    pip install pytz


Setup
=====
### MySQL Destination
1. For MySQL destination, run the ./sql/create.sql statements on your target 
schema.
2. Copy config-example.py to config.py and fill out the appropriate connection 
details.

### CSV Destination
None


Usage
=====
Run the following command for usage details:

    python import_history.py --help
    
The --help documentation gives a brief summary of all required and optional 
parameters. An example for importing hourly, historical weather information 
from November 1, 2010 through January 31, 2011 from the Kitchener/Waterloo 
weather station:

    python import_history.py --station_id=48569 --year_start 2010 --month_start 11 --year_end 2011 --month_end 1 --tz_name America/Toronto --dest csv

On notable parameter is the integer passed to --station_id. This corresponds 
to an Environment Canada weather station. Use form on the "Search by 
Proximity" tab on [Environment Canada's Advanced Search](http://climate.weather.gc.ca/advanceSearch/searchHistoricData_e.html) 
page to find the nearest weather station to your city of interest.

1. Pull up an appropriate city from the "Select City" drop-down and hit 
"Search".
2. On the listing of nearby weather stations, click the "Go" button of the 
weather station of your choice that has "Hourly" observations.
3. In the resulting page, grab the number in the &StationID= URL parameter.