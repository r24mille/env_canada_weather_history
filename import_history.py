from urllib import request
from xml.etree import ElementTree
from models import StationData

def fetch_content(station_id, year_num, month_num, day_num_start,
                  timeframe=1, frmt='xml'):
    """ Fetch a single CSV document from Environment Canada.
    
    TODO: Document function parameters.
    """
    data_url = ('http://climate.weather.gc.ca/climateData/bulkdata_e.html' + 
               '?format=' + frmt + 
               '&stationID=' + str(station_id) + 
               '&Year=' + str(year_num) + 
               '&Month=' + str(month_num) + 
               '&Day=' + str(day_num_start) + 
               '&timeframe=' + str(timeframe))
    print(data_url)
    url_response = request.urlopen(data_url)
    return url_response

def import_xml(station_id, year_num, month_num, day_num_start,
               local_standard_tz):
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
    local_standard_tz -- A three character string representing the 
                         timezone (in standard time) local to the station_id
                         being requested.
                         
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
    
    # Instantiate list to store StationData objects
    station_datas = list()
    
    # Iterate stationdata XML elements and append StationData objects to list
    for sd_elmnt in weather_root.iter('stationdata'):
        station_data = StationData()
        
        # TODO get datetime data from attributes
        # print(sd_elmnt.attrib)
        # station_data.obs_datetime_std = 
        # station_data.obs_quality = 
        
        # Set StationData fields based on child elements' values
        station_data.station_id = station_id
        
        temp_txt = sd_elmnt.find('temp').text
        if temp_txt and temp_txt != ' ':
            station_data.temp_c = float(temp_txt)
        
        dptemp_txt = sd_elmnt.find('dptemp').text
        if dptemp_txt and dptemp_txt != ' ':
            station_data.dewpoint_temp_c = float(dptemp_txt)
        
        relhum_txt = sd_elmnt.find('relhum').text
        if relhum_txt and relhum_txt != ' ':
            station_data.rel_humidity_pct = int(relhum_txt)
            
        winddir_txt = sd_elmnt.find('winddir').text
        if winddir_txt and winddir_txt != ' ':
            station_data.wind_dir_deg = int(winddir_txt) * 10
            
        windspd_txt = sd_elmnt.find('windspd').text
        if windspd_txt and windspd_txt != ' ':
            station_data.wind_speed_kph = int(windspd_txt)
            
        visibility_txt = sd_elmnt.find('visibility').text
        if visibility_txt and visibility_txt != ' ':
            station_data.visibility_km = float(visibility_txt)
            
        stnpress_txt = sd_elmnt.find('stnpress').text
        if stnpress_txt and stnpress_txt != ' ':
            station_data.station_pressure_kpa = float(stnpress_txt)
            
        humidex_txt = sd_elmnt.find('humidex').text
        if humidex_txt and humidex_txt != ' ':
            station_data.humidex = float(humidex_txt)
            
        windchill_txt = sd_elmnt.find('windchill').text
        if windchill_txt and windchill_txt != ' ':
            station_data.wind_chill = int(windchill_txt)
            
        station_data.weather_desc = sd_elmnt.find('weather').text
        station_data.time_zone = local_standard_tz
        
        # Add StationData element to list
        station_datas.append(station_data)
    
    # Return XML elements parsed into a list of StationData objects
    return station_datas
        
        
if __name__ == "__main__":
    # station_data = StationData()
    # station_data.station_id = 32008
    # execute only if run as a script
    station_datas = import_xml(station_id=32008, year_num=2010, month_num=1,
                               day_num_start=1, local_standard_tz='EST')
    
    for station_data in station_datas:
        print(station_data)
