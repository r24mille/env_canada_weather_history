-- Create the table which holds stationinformation data
CREATE TABLE `envcan_station` ( 
  `stationID` MEDIUMINT UNSIGNED NOT NULL, 
  `name` VARCHAR(100) NULL, 
  `province` VARCHAR(45) NULL, 
  `latitude` FLOAT NULL, 
  `longitude` FLOAT NULL, 
  `elevation` FLOAT NULL, 
  `climate_identifier` MEDIUMINT UNSIGNED NULL, 
  `local_timezone` VARCHAR(45) NULL, 
  PRIMARY KEY (`stationID`)) 
ENGINE = InnoDB 
CHARACTER SET utf8;

-- Create the table which holds legend content (ie. descriptions for 'quality')
CREATE TABLE `envcan_legend` ( 
  `envcan_quality` CHAR(2) NOT NULL, 
  `description` VARCHAR(100) NULL, 
  PRIMARY KEY (`envcan_quality`)) 
Engine=InnoDB 
CHARACTER SET utf8;

INSERT INTO envcan_legend(envcan_quality, description) 
VALUES ('M', 'Missing'), ('E', 'Estimated'), ('NA', 'Not Available'), 
('**', 'Partner data that is not subject to review by the National Climate Archives');

-- Create the table which holds stationdata content (ie. historical weather 
-- observations).
CREATE TABLE `envcan_observation` ( 
  `envcan_obs_id` INT UNSIGNED NOT NULL AUTO_INCREMENT, 
  `stationID` MEDIUMINT UNSIGNED NOT NULL, 
  `obs_datetime_std` DATETIME NULL, 
  `obs_datetime_dst` DATETIME NULL, 
  `temp_c` FLOAT NULL, 
  `dewpoint_temp_c` FLOAT NULL, 
  `rel_humidity_pct` TINYINT UNSIGNED NULL, 
  `wind_dir_deg` SMALLINT UNSIGNED NULL, 
  `wind_speed_kph` SMALLINT UNSIGNED NULL, 
  `visibility_km` FLOAT NULL, 
  `station_pressure_kpa` FLOAT NULL, 
  `humidex` FLOAT NULL, 
  `wind_chill` SMALLINT NULL, 
  `weather_desc` VARCHAR(75) NULL, 
  `quality` CHAR(2) NULL, 
  PRIMARY KEY (`envcan_obs_id`), 
  INDEX `envcan_station_fk_idx` (`stationID` ASC), 
  INDEX `station_dt_std_idx` (`stationID` ASC, `obs_datetime_std` ASC), 
  INDEX `station_dt_dst_idx` (`stationID` ASC, `obs_datetime_dst` ASC), 
  CONSTRAINT `envcan_station_fk` 
    FOREIGN KEY (`stationID`) 
    REFERENCES `envcan_station` (`stationID`) 
    ON DELETE NO ACTION 
    ON UPDATE NO ACTION, 
  CONSTRAINT `envcan_legend_fk` 
    FOREIGN KEY (`quality`) 
    REFERENCES `envcan_legend` (`envcan_quality`) 
    ON DELETE NO ACTION 
    ON UPDATE NO ACTION) 
Engine=InnoDB 
CHARACTER SET utf8;