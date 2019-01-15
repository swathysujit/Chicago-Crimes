#Crime and Weather Base Table Creation
#Creating the base table and importing crime and weather data(csv file) for creation of the dimensional model

DROP TABLE mydb.crime_weather_base_table;

CREATE TABLE IF NOT EXISTS `crime_weather_base_table` (
`ID` VARCHAR(10) NOT NULL,
`Date` DATETIME,		
`Block` VARCHAR(50),	
`IUCR` VARCHAR(20),	
`PrimaryType` VARCHAR(50),		
`LocationDesc` VARCHAR(100),	
`District` INT(10),	
`Ward` INT(10),
`XCoord` INT(10),	
`YCoord` INT(10),	
`Year` INT(4),	
`Latitude` FLOAT,	
`Longitude` FLOAT,	
`WindAvg` DECIMAL(6,3),	
`Precipitation` DECIMAL(6,3),	
`Snow` DECIMAL(6,3),	
`SnowDepth` DECIMAL(6,3),	
`TempAvg` DECIMAL(6,3),	
`TempMax` DECIMAL(6,3),	
`Tmin` DECIMAL(6,3),	
`IndFog` INT(2),	
`IndHeavyFog` INT(2),	
`IndThunder` INT(2),	
`IndPellets` INT(2),	
`IndGlaze` INT(2),	
`IndSmoke` INT(2),	
`IndDriftSnow` INT(2),	
`Arrest` INT(2),	
`Domestic` INT(2),
PRIMARY KEY (`ID`)
) ENGINE=InnoDB;

#Instructions for loading csv data into mysql :
#Commented "secure_file_priv" statement in the my configuration file at C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ location
#Uncommented and restart mysql after importing, so in case we need to import again, comment that line and run import statements

SHOW VARIABLES LIKE 'secure_file_priv';
SET GLOBAL local_infile = 'ON';

#Inserting data from csv file into the base table created above
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Crime_Weather_Cleaned_2017.csv' 
INTO TABLE mydb.crime_weather_base_table
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

#Checking number of rows imported into the base_table
SELECT count(*) from mydb.crime_weather_base_table;
#Total number of rows = 264,997