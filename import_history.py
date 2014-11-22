from urllib import request
from xml.etree import ElementTree

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
    xml_string = url_response.read().decode('utf-8')
    weather_root = ElementTree.fromstring(xml_string)
    
    for stationdata in weather_root.iter('stationdata'):
        print(stationdata.attrib)
        print(stationdata.find('temp').text)
        
if __name__ == "__main__":
    # execute only if run as a script
    fetch_content(station_id=32008, year_num=2010, month_num=1,
                  day_num_start=1)
