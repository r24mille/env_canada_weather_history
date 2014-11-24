from datetime import datetime, timedelta, timezone
from urllib import request
from xml.etree import ElementTree

import pytz

from models import Observation, Station


def fetch_content(station_id, year_num, month_num, day_num_start,
                  timeframe=1, frmt='xml'):
    """
    Fetch weather history data from Environment Canada.
    
    TODO(r24mille): Allow user to adjust timeframe parameter.
    TODO(r24mille): Allow a user to switch between XML/CSV data for parsing.
    
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

def import_xml(station_id, year_num, month_num, day_num_start,
               local_tz_name):
    """
    Calls Environment Canada endpoint and parses the returned XML into 
    StationData objects.
    
    Keyword arguments:
    station_id -- Integer corresponding to an Environment Canada station ID 
                  (ie. location of weather reading).
    year_num -- Integer indicating the year of the requested data.
    month_num -- Integer indicating the month of the requested data.
    day_num_start -- Integer indicating the starting day of the forecast, 
                     though multiple days of forecasted data may be returned.
    local_tz_name -- String representation of local timezone name 
                    (eg. 'America/Toronto').
                         
    Return:
    A list of StationData objects.
    """
    xml_response = fetch_content(station_id=station_id, year_num=year_num,
                                month_num=month_num,
                                day_num_start=day_num_start, timeframe=1,
                                frmt='xml')
    xml_string = xml_response.read().decode('utf-8')
    print(xml_string)
    weather_root = ElementTree.fromstring(xml_string)
    
    # Instantiate objects that are returned by this function
    station = Station()
    observations = list()
    
    # A few values that apply to all observations
    station.station_id = station_id
    station.local_tz_str = local_tz_name
    station_local_tz = pytz.timezone(local_tz_name)
    offset_delta = station_local_tz.utcoffset(datetime.utcfromtimestamp(0))
    station_std_tz = timezone(offset_delta)
    for si_elmnt in weather_root.iter('stationinformation'):
        latitude_txt = si_elmnt.find('latitude').text
        if latitude_txt and latitude_txt != ' ':
            station.latitude = float(latitude_txt)
        
        longitude_txt = si_elmnt.find('longitude').text
        if longitude_txt and longitude_txt != ' ':
            station.longitude = float(longitude_txt)
            
        elevation_txt = si_elmnt.find('elevation').text
        if elevation_txt and elevation_txt != ' ':
            station.elevation = float(elevation_txt)
    
    # Iterate stationdata XML elements and append StationData objects to list
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

        # observation.obs_datetime_std = 
        quality_txt = sd_elmnt.attrib['quality']
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
    
    # Return XML elements parsed into a list of StationData objects
    return [station, observations]
        
        
if __name__ == "__main__":
    [station, observations] = import_xml(station_id=32008, year_num=2010, 
                                         month_num=3, day_num_start=1, 
                                         local_tz_name='America/Toronto')
    
    for observation in observations:
        print(observation)
        
    print(observations[0])
