#Creating the dimensional model - Star Schema 

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

# use crime_star;
DROP SCHEMA IF EXISTS crime_star;

#Creating the schema
CREATE SCHEMA IF NOT EXISTS `crime_star` DEFAULT CHARACTER SET latin1 ;
USE `crime_star` ;

#Creating the star schema dimension tables
#Location Dimension
DROP TABLE IF EXISTS crime_star.dim_location;

CREATE TABLE IF NOT EXISTS `crime_star`.`dim_location` (
`location_key` INT(10) NOT NULL AUTO_INCREMENT,
`Block` VARCHAR(50),
`LocationDesc` VARCHAR(100),
`District` INT(10),	
`Ward` INT(10),	
`XCoord` INT(10),	
`YCoord` INT(10),
PRIMARY KEY (`location_key`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Weather Dimension
DROP TABLE IF EXISTS crime_star.dim_weather;
CREATE TABLE IF NOT EXISTS `crime_star`.`dim_weather` (
`weather_key` INT(10) NOT NULL AUTO_INCREMENT,
`WindAvg` DECIMAL(6,3),	
`Precipitation` DECIMAL(6,3),	
`Snow` DECIMAL(6,3),	
`SnowDepth` DECIMAL(6,3),	
`TempMax` DECIMAL(6,3),	
`TempMin` DECIMAL(6,3),	
`IndFog` INT(2),	
`IndHeavyFog` INT(2),	
`IndThunder` INT(2),	
`IndPellets` INT(2),	
`IndGlaze` INT(2),	
`IndSmoke` INT(2),	
`IndDriftSnow` INT(2),
PRIMARY KEY (`weather_key`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Crime Dimension
DROP TABLE IF EXISTS crime_star.dim_crime;

CREATE TABLE IF NOT EXISTS `crime_star`.`dim_crime` (
`IUCR_key` INT(10) NOT NULL AUTO_INCREMENT,
`IUCR` VARCHAR(20),	
`PrimaryType` VARCHAR(100),
PRIMARY KEY (`IUCR_key`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Date Dimension
DROP TABLE IF EXISTS crime_star.dim_date;

CREATE TABLE IF NOT EXISTS `crime_star`.`dim_date` (
`date_key` BIGINT(20) NOT NULL AUTO_INCREMENT,
`datetime` DATETIME NOT NULL,
`date` DATE NULL DEFAULT NULL,
`weekend` CHAR(10) NOT NULL DEFAULT 'Weekday',
`day_of_week` CHAR(10) NULL DEFAULT NULL,
`month` CHAR(10) NULL DEFAULT NULL,
`year` INT(11) NULL DEFAULT NULL,
PRIMARY KEY (`date_key`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

#Creating the Crime Fact Table with references to all dimension tables and foreign key contraints
DROP TABLE IF EXISTS crime_star.fact_crime;

CREATE TABLE IF NOT EXISTS `crime_star`.`fact_crime`(
	`ID` VARCHAR(10) NOT NULL,
	`date_key` BIGINT(20) NOT NULL,
    `location_key` INT(10) NOT NULL,
    `weather_key` INT(10) NOT NULL,
    `IUCR_key` INT(10) NOT NULL,
    `Arrest` INT(2),
    `Domestic` INT(2),
    `Crime_Count` INT(2),
    `Latitude` FLOAT,
    `Longitude` FLOAT,
    `TempAvg` DECIMAL(6,3),
	PRIMARY KEY (`ID`),
	INDEX `dim_crime_fk` (`IUCR_key` ASC),
	INDEX `dim_location_fk` (`location_key` ASC),
	INDEX `dim_weather_fk` (`weather_key` ASC),
	INDEX `dim_date_fk` (`date_key` ASC),
	CONSTRAINT `fk_factcrime_dim_crime`
		FOREIGN KEY (`IUCR_key`)
		REFERENCES `crime_star`.`dim_crime` (`IUCR_key`)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION,
	CONSTRAINT `fk_factcrime_dim_location`
		FOREIGN KEY (`location_key`)
		REFERENCES `crime_star`.`dim_location` (`location_key`)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION,
	CONSTRAINT `fk_factcrime_dim_weather`
		FOREIGN KEY (`weather_key`)
		REFERENCES `crime_star`.`dim_weather` (`weather_key`)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION,
	CONSTRAINT `fk_factcrime_dim_date`
		FOREIGN KEY (`date_key`)
		REFERENCES `crime_star`.`dim_date` (`date_key`)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
    )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;