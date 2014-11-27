import argparse
import csv
from datetime import datetime, timezone
from urllib import request
from xml.etree import ElementTree

import mysql.connector 
import pytz

from config import mysql_config
from models import Observation, Station

def canadian_timezones():
    """
    Valid set of Canadian timezone strings according to the IANA standard.
    See https://en.wikipedia.org/wiki/Time_in_Canada#IANA_time_zone_database
    
    Return
    List of valid, Canadian timezone strings.
    """
    
    timezones = ['America/St_Johns', 'America/Halifax', 'America/Glace_Bay', 
                 'America/Moncton', 'America/Goose_Bay', 
                 'America/Blanc-Sablon', 'America/Montreal', 
                 'America/Toronto', 'America/Nipigon', 'America/Thunder_Bay', 
                 'America/Iqaluit', 'America/Pangnirtung', 'America/Resolute', 
                 'America/Atikokan', 'America/Rankin_Inlet', 
                 'America/Winnipeg', 'America/Rainy_River', 'America/Regina', 
                 'America/Swift_Current', 'America/Edmonton', 
                 'America/Cambridge_Bay', 'America/Yellowknife', 
                 'America/Inuvik', 'America/Creston', 'America/Dawson_Creek', 
                 'America/Vancouver', 'America/Whitehorse', 'America/Dawson']
    return timezones

def csv_write_station(station, filename='station.csv'):
    """Write model.Station information to CSV file."""
    with open(filename, 'w', newline='') as csvfile:
        csvw = csv.writer(csvfile)
        csvw.writerow(['station_id', 'name', 'province', 'longitude', 
                       'latitude', 'elevation', 'climate_identifier', 
                       'local_tz_str'])
        csvw.writerow([station.station_id, station.name, station.province, 
                       station.longitude, station.latitude, station.elevation, 
                       station.climate_identifier, station.local_tz_str])
    
    
def csv_write_observations(observations, filename='observations.csv'):
    """Write model.Observation list to CSV file."""
    with open(filename, 'w', newline='') as csvfile:
        csvw = csv.writer(csvfile)
        csvw.writerow(['station_id', 'temp_c', 'dewpoint_temp_c', 
                       'rel_humidity_pct', 'wind_dir_deg', 'wind_speed_kph', 
                       'visibility_km', 'station_pressure_kpa', 'humidex',
                       'wind_chill', 'weather_desc', 'obs_datetime_std', 
                       'obs_datetime_dst', 'obs_quality'])
        for obs in observations:
            csvw.writerow([obs.station_id, obs.temp_c, obs.dewpoint_temp_c, 
                           obs.rel_humidity_pct, obs.wind_dir_deg, 
                           obs.wind_speed_kph, obs.visibility_km, 
                           obs.station_pressure_kpa, obs.humidex, 
                           obs.wind_chill, obs.weather_desc, 
                           obs.obs_datetime_std, obs.obs_datetime_dst, 
                           obs.obs_quality])

def fetch_content(station_id, year_num, month_num, day_num_start,
                  timeframe=1, frmt='xml'):
    """
    Fetch weather history data from Environment Canada.
    
    TODO(r24mille): Allow a user to switch between XML/CSV data for parsing.
    TODO(r24mille): Allow user to change the timeframe parameter for different 
                    data resolution (ie. daily, hourly, etc.)
    
    Keyword arguments:
    station_id -- Integer corresponding to an Environment Canada station ID 
                  (ie. location of weather reading).
    year_num -- Integer indicating the year of the requested data.
    month_num -- Integer indicating the month of the requested data.
    day_num_start -- Integer indicating the starting day of the forecast, 
                     though multiple days of forecasted data may be returned.
    timeframe -- Controls the time span of data that is returned 
                 (default 1=month of hourly observations).
    frmt -- Controls the format that Environment Canada data should be 
            returned in (default 'xml').
                     
    Return:
    The request.urlopen response.
    """
    data_url = ('http://climate.weather.gc.ca/climateData/bulkdata_e.html' + 
               '?format=' + frmt + 
               '&stationID=' + str(station_id) + 
               '&Year=' + str(year_num) + 
               '&Month=' + str(month_num) + 
               '&Day=' + str(day_num_start) + 
               '&timeframe=' + str(timeframe))
    print('URL: ' + data_url)
    url_response = request.urlopen(data_url)
    return url_response

def sql_insert_observations(observations, config, batch_size=100):
    """
    Inserts Observations (ie. stationdata) into a database.
    
    TODO (r24mille): Decouple this function from the main file
    TODO (r24mille): Support some other driver other than mysql.connector
    
    Keyword arguments:
    observation -- A list of models.Observation objects to be inserted into a 
                   MySQL database
    config -- A dict of MySQL configuration credentials (see config-example.py)
    batch_size -- INSERT min(batch_size, len(observations)) rows at a time for 
                  fairly fast INSERT times (default batch_size=100).
    """
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    
    # Batch size of query
    insert_complete = False
    batch_start_idx = 0
    batch_size = min(batch_size, len(observations))
    
    # Continue batched INSERTs until Observations list has been processed
    while insert_complete == False:
        batch_idx_upperbound = (batch_start_idx + batch_size)
        ins_data = ()
        ins_obs = ("INSERT INTO envcan_observation (stationID, " + 
                   "obs_datetime_std, obs_datetime_dst, temp_c, " + 
                   "dewpoint_temp_c, rel_humidity_pct, wind_dir_deg, " + 
                   "wind_speed_kph, visibility_km, station_pressure_kpa, " + 
                   "humidex, wind_chill, weather_desc, quality) VALUES ")
        for i in range(batch_start_idx, batch_idx_upperbound):
            obs = observations[i]
            ins_obs += "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            ins_data += (obs.station_id,
                         obs.obs_datetime_std.strftime('%Y-%m-%d %H:%M:%S'),
                         obs.obs_datetime_dst.strftime('%Y-%m-%d %H:%M:%S'),
                         obs.temp_c, obs.dewpoint_temp_c,
                         obs.rel_humidity_pct, obs.wind_dir_deg,
                         obs.wind_speed_kph, obs.visibility_km,
                         obs.station_pressure_kpa, obs.humidex, obs.wind_chill,
                         obs.weather_desc, obs.obs_quality)
            
            # If i isn't the last item in batch, add a comma to the VALUES items
            if i != (batch_idx_upperbound - 1):
                ins_obs += ", "
        
        cursor.execute(ins_obs, ins_data)
        
        # If the upper bound is the last observation, mark INSERTs complete
        if len(observations) - batch_idx_upperbound == 0:
            insert_complete = True
        else:  # Slide batch window
            batch_size = min(batch_size,
                             len(observations) - batch_idx_upperbound)
            batch_start_idx = batch_idx_upperbound
    
    # Make sure data is committed to the database
    cnx.commit()
    cursor.close()
    cnx.close()

def sql_insert_station(station, config):
    """
    Checks if a station matching the stationID exists. If no match exists, 
    then one is inserted.
    
    TODO (r24mille): Decouple this function from the main file
    TODO (r24mille): Support some other driver other than mysql.connector
    
    Keyword arguments:
    station -- A models.Station object to be inserted into a MySQL database
    config -- A dict of MySQL configuration credentials (see config-example.py)
    """
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    # Query envcan_station for matching stationID
    query_station = ("SELECT * FROM envcan_station " + 
                     "WHERE stationID = %(station_id)s")
    cursor.execute(query_station, {'station_id':station.station_id})
    
    station_row = cursor.fetchone()
    # If no station exists matching that stationID, insert one
    if station_row == None:
        insert_station = ("INSERT INTO envcan_station (stationID, name, " + 
                          "province, latitude, longitude, elevation, " + 
                          "climate_identifier, local_timezone) " + 
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
        insert_data = (station.station_id, station.name, station.province,
                    station.latitude, station.longitude, station.elevation,
                    station.climate_identifier, station.local_tz_str)
        cursor.execute(insert_station, insert_data)
    
    # Make sure data is committed to the database
    cnx.commit()
    cursor.close()
    cnx.close()

def range_hourly(station_id, year_start, year_end, month_start, month_end,
                 day_start, local_tz_name):
    """
    Calls Environment Canada endpoint and parses the returned XML into 
    StationData objects.
        
    Keyword arguments:
    station_id -- Integer corresponding to an Environment Canada station ID 
                  (ie. location of weather reading).
    year_start -- Integer indicating the year of the first weather history
                  request.
    year_end -- Integer indicating the year of the last weather history 
                request (inclusive). In combination with month_start and 
                month_end, all weather history between start and end times 
                will be requested.
    month_start -- Integer indicating the month of the first weather history 
                   request.
    month_end -- Integer indicating the month of the last weather history 
                 request (inclusive). In combination with year_start and 
                 year_end, all weather history between start and end times 
                 will be requested.
    day_start -- Integer indicating the starting day of the forecast, 
                 though multiple days of forecasted data will be returned.
    local_tz_name -- String representation of local timezone name 
                     (eg. 'America/Toronto').
                         
    Return:
    Two two-item vector [station, observations] where station is a 
    model.Station object and observations is a list of hourly 
    model.Observation objects.
    """
    # Instantiate objects that are returned by this function
    station = None
    observations = list()
    
    y = year_start
    m = month_start
    d = day_start
    req_date = datetime(y, m, d)
    end_date = datetime(year_end, month_end, day_start)
    while req_date <= end_date:
        xml_response = fetch_content(station_id=station_id, year_num=y,
                                    month_num=m, day_num_start=d, timeframe=1,
                                    frmt='xml')
        xml_string = xml_response.read().decode('utf-8')
        weather_root = ElementTree.fromstring(xml_string)
        
        # Only populate Station once
        if station == None:
            station = Station()
            station.station_id = station_id
            station.local_tz_str = local_tz_name
            station_local_tz = pytz.timezone(local_tz_name)
            epoch = datetime.utcfromtimestamp(0)
            offset_delta = station_local_tz.utcoffset(epoch)
            station_std_tz = timezone(offset_delta)
            for si_elmnt in weather_root.iter('stationinformation'):
                name_txt = si_elmnt.find('name').text
                if name_txt and name_txt != ' ':
                    station.name = name_txt
                    
                province_txt = si_elmnt.find('province').text
                if province_txt and province_txt != ' ':
                    station.province = province_txt
                
                latitude_txt = si_elmnt.find('latitude').text
                if latitude_txt and latitude_txt != ' ':
                    station.latitude = float(latitude_txt)
                
                longitude_txt = si_elmnt.find('longitude').text
                if longitude_txt and longitude_txt != ' ':
                    station.longitude = float(longitude_txt)
                    
                elevation_txt = si_elmnt.find('elevation').text
                if elevation_txt and elevation_txt != ' ':
                    station.elevation = float(elevation_txt)
                    
                climate_id_txt = si_elmnt.find('climate_identifier').text
                if climate_id_txt and climate_id_txt != ' ':
                    station.climate_identifier = int(climate_id_txt)
        
        # Iterate stationdata XML elements and append Observations to list
        for sd_elmnt in weather_root.iter('stationdata'):
            observation = Observation()
            
            # Get portions of date_time for observation
            year_txt = sd_elmnt.attrib['year']
            month_txt = sd_elmnt.attrib['month']
            day_txt = sd_elmnt.attrib['day']
            hour_txt = sd_elmnt.attrib['hour']
            minute_txt = sd_elmnt.attrib['minute']
            if year_txt and month_txt and day_txt and hour_txt and minute_txt:
                observation.obs_datetime_std = datetime(year=int(year_txt),
                                                         month=int(month_txt),
                                                         day=int(day_txt),
                                                         hour=int(hour_txt),
                                                         minute=int(minute_txt),
                                                         second=0,
                                                         microsecond=0,
                                                         tzinfo=station_std_tz)
                observation.obs_datetime_dst = observation.obs_datetime_std.astimezone(station_local_tz)
    
            if 'quality' in sd_elmnt.attrib:
                quality_txt = sd_elmnt.attrib['quality']
            else:
                quality_txt = None
            if quality_txt and quality_txt != ' ':
                observation.obs_quality = quality_txt
            
            # Set StationData fields based on child elements' values
            observation.station_id = station_id
            
            temp_txt = sd_elmnt.find('temp').text
            if temp_txt and temp_txt != ' ':
                observation.temp_c = float(temp_txt)
            
            dptemp_txt = sd_elmnt.find('dptemp').text
            if dptemp_txt and dptemp_txt != ' ':
                observation.dewpoint_temp_c = float(dptemp_txt)
            
            relhum_txt = sd_elmnt.find('relhum').text
            if relhum_txt and relhum_txt != ' ':
                observation.rel_humidity_pct = int(relhum_txt)
                
            winddir_txt = sd_elmnt.find('winddir').text
            if winddir_txt and winddir_txt != ' ':
                observation.wind_dir_deg = int(winddir_txt) * 10
                
            windspd_txt = sd_elmnt.find('windspd').text
            if windspd_txt and windspd_txt != ' ':
                observation.wind_speed_kph = int(windspd_txt)
                
            visibility_txt = sd_elmnt.find('visibility').text
            if visibility_txt and visibility_txt != ' ':
                observation.visibility_km = float(visibility_txt)
                
            stnpress_txt = sd_elmnt.find('stnpress').text
            if stnpress_txt and stnpress_txt != ' ':
                observation.station_pressure_kpa = float(stnpress_txt)
                
            humidex_txt = sd_elmnt.find('humidex').text
            if humidex_txt and humidex_txt != ' ':
                observation.humidex = float(humidex_txt)
                
            windchill_txt = sd_elmnt.find('windchill').text
            if windchill_txt and windchill_txt != ' ':
                observation.wind_chill = int(windchill_txt)
                
            observation.weather_desc = sd_elmnt.find('weather').text
            
            # Add StationData element to list
            observations.append(observation)
    
        # Increment year and month to populate date range
        if m < 12:
            m += 1
        else:
            y += 1
            m = 1
        req_date = datetime(y, m, d)
    
    # Return XML elements parsed into a list of StationData objects
    return [station, observations]


def main():
    """
    Main method intended to by called via command-line
    
    TODO(r24mille): Parse SQL config from file rather than Python function
    TODO(r24mille): Add means to customize CSV filenames generated
    """
    
    description = 'Environment Canda historical weather parser/import tool.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--station_id', required=True, type=int,
                        help='The stationID from climate.weather.gc.ca URL')
    parser.add_argument('--year_start', required=True, type=int,
                        help='Year for start of historical weather range')
    parser.add_argument('--year_end', required=True, type=int,
                        help='Year for end of historical weather range (inclusive)')
    parser.add_argument('--month_start', required=True, type=int,
                        help='Month for start of historical weather range')
    parser.add_argument('--month_end', required=True, type=int,
                        help='Month for end of historical weather range (inclusive)')
    parser.add_argument('--tz_name', required=True, type=str, 
                        choices=canadian_timezones(), 
                        help='IANA timezone string of the weather station')
    parser.add_argument('--day_start', default=1, type=int,
                        help='Starting day for the historical weather range')
    parser.add_argument('--dest', default='csv', type=str, 
                        choices=['csv', 'sql'],
                        help='Destination of the parsed weather information')
    parser.add_argument('--batch_size', default=100, type=int,
                        help='If destination is SQL, control the INSERT batch size')
    args = parser.parse_args()
    
    print(args)        
    
    # Fetch range of hourly weather observations
    [station, observations] = range_hourly(station_id=args.station_id,
                                           year_start=args.year_start,
                                           year_end=args.year_end, 
                                           month_start=args.month_start,
                                           month_end=args.month_end, 
                                           day_start=args.day_start,
                                           local_tz_name=args.tz_name)
 
    # Write parsed information to appropriate destination
    if args.dest == 'sql':
        sql_insert_station(station=station, config=mysql_config())
        sql_insert_observations(observations=observations, 
                                config=mysql_config(), 
                                batch_size=args.batch_size)
    elif args.dest == 'csv':
        csv_write_station(station=station, 
                          filename=(str(args.station_id) + 
                                    'station.csv'))
        csv_write_observations(observations=observations, 
                               filename=(str(args.station_id) + 
                                         'observations.csv'))


if __name__ == "__main__":
    main()