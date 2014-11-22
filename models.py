from datetime import datetime

class StationData():
    """
    Create an object structured similar to the stationdata XML object so 
    that data retrieved via the XML data source is easier to manipulate.
    """    
    def __init__(self):
        self.station_id = None
        self.longitude = None
        self.latitude = None
        self.elevation = None
        self.temp_c = None
        self.dewpoint_temp_c = None
        self.rel_humidity_pct = None
        self.wind_dir_deg = None
        self.wind_speed_kph = None
        self.visibility_km = None
        self.station_pressure_kpa = None
        self.humidex = None
        self.wind_chill = None
        self.weather_desc = None
        self.time_zone = None
        self.obs_datetime_std = None
        self.obs_quality = None
        
    def __str__(self):
        return ('StationData values [station_id=' + 
                str(self.station_id or 'None') + 
                ', longitude=' + str(self.longitude or 'None') + 
                ', latitude=' + str(self.latitude or 'None') + 
                ', elevation=' + str(self.elevation or 'None') + 
                ', temp_c=' + str(self.temp_c or 'None') + 
                ', dewpoint_temp_c=' + str(self.dewpoint_temp_c or 'None') + 
                ', rel_humidity_pct=' + str(self.rel_humidity_pct or 'None') + 
                ', wind_dir_deg=' + str(self.wind_dir_deg or 'None') + 
                ', wind_speed_kph=' + str(self.wind_speed_kph or 'None') + 
                ', visibility_km=' + str(self.visibility_km or 'None') + 
                ', station_pressure_kpa=' + 
                str(self.station_pressure_kpa or 'None') + 
                ', humidex=' + str(self.humidex or 'None') + 
                ', wind_chill=' + str(self.wind_chill or 'None') + 
                ', weather_desc=' + str(self.weather_desc or 'None') + 
                ', time_zone=' + str(self.time_zone or 'None') + 
                ', obs_datetime_std=' + str(self.obs_datetime_std or 'None') + 
                ', obs_quality=' + str(self.obs_quality or 'None') + ']')
