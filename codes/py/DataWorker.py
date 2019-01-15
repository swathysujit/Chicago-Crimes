"""
@file_name : DataWorker.py
@author : Srihari Seshadri
@description : This file does the following operations :
                1.
            Potential upgrades include
                1. Analysis modules
@date : 11-14-2018
"""

import os
import pandas as pd
import numpy as np
from DataExtractor import DataExtractor
from datetime import datetime


class DataWorker:

    def __init__(self):
        pass

    def _get_dim_date(self, dataframe):
        date_cols = ['Date']
        dim_date = dataframe[date_cols]
        dim_date = dim_date.drop_duplicates()
        dim_date['date_key'] = range(1, dim_date.shape[0] + 1, 1)
        # Make the other useful columns
        # date column
        dim_date['date'] = [datetime.strptime(dt, '%Y-%m-%d %H:%M:%S').date()
                            for dt in
                            dim_date['Date']]
        # Weekend
        dim_date['weekend'] = [dt.weekday() for dt in dim_date['date']]
        dim_date['weekend'] = ['weekday' if dt < 5 else 'weekend'
                               for dt in dim_date['weekend']]
        # Day of week
        dim_date['day_of_week'] = [dt.strftime("%A") for dt in dim_date['date']]

        # Month
        dim_date['month'] = [dt.month for dt in dim_date['date']]

        # year
        dim_date['year'] = [dt.year for dt in dim_date['date']]

        return dim_date

    def _get_dim_location(self, dataframe):
        # Create the Location Dim table
        loc_cols = ['Block', 'LocationDesc', 'District', 'Ward', 'XCoord',
                    'YCoord']
        dim_location = dataframe[loc_cols]
        dim_location = dim_location.drop_duplicates()
        dim_location['location_key'] = range(1, dim_location.shape[0] + 1, 1)

        return dim_location

    def _get_dim_crime(self, dataframe):
        # Create the crime dim table
        crime_cols = ['IUCR', 'PrimaryType']
        dim_crime = dataframe[crime_cols]
        dim_crime = dim_crime.drop_duplicates()
        dim_crime['IUCR_key'] = range(1, dim_crime.shape[0] + 1, 1)

        return dim_crime

    def _get_dim_weather(self, dataframe):
        # Create the weather dim table
        weather_cols = ['WindAvg', 'Precipitation', 'Snow', 'SnowDepth',
                        'TempMax',
                        'Tmin', 'IndFog', 'IndHeavyFog', 'IndThunder',
                        'IndPellets',
                        'IndGlaze', 'IndSmoke', 'IndDriftSnow']
        dim_weather = dataframe[weather_cols]
        dim_weather = dim_weather.drop_duplicates()
        dim_weather['weather_key'] = range(1, dim_weather.shape[0] + 1, 1)

        return dim_weather

    def _get_fact_table(self,
                        base_table,
                        dim_date,
                        dim_location,
                        dim_crime,
                        dim_weather):

        # Merge date tables
        basedate_merge_cols = ['Date']
        date_merge_cols = ['Date']

        # Decide the columns we want
        temp = list(base_table.columns)
        temp.append("date_key")

        merged_df = pd.merge(left=base_table,
                             right=dim_date,
                             how='left',
                             left_on=basedate_merge_cols,
                             right_on=date_merge_cols)[temp]

        # 2. merged1 LJ dim_location

        baseloc_merge_cols = ['Block', 'LocationDesc', 'District', 'Ward',
                              'XCoord',
                              'YCoord']
        loc_merge_cols = ['Block', 'LocationDesc', 'District', 'Ward', 'XCoord',
                          'YCoord']

        # Decide the columns we want
        temp = list(merged_df.columns)
        temp.append("location_key")

        merged_df = pd.merge(left=merged_df,
                             right=dim_location,
                             how='left',
                             left_on=baseloc_merge_cols,
                             right_on=loc_merge_cols)[temp]

        # 3. merged1 LJ dim_weather

        baseweather_merge_cols = ['WindAvg', 'Precipitation', 'Snow',
                                  'SnowDepth',
                                  'TempMax', 'Tmin', 'IndFog', 'IndHeavyFog',
                                  'IndThunder', 'IndPellets', 'IndGlaze',
                                  'IndSmoke',
                                  'IndDriftSnow']

        weather_merge_cols = ['WindAvg', 'Precipitation', 'Snow', 'SnowDepth',
                              'TempMax', 'Tmin', 'IndFog', 'IndHeavyFog',
                              'IndThunder', 'IndPellets', 'IndGlaze',
                              'IndSmoke',
                              'IndDriftSnow']

        # Decide the columns we want
        temp = list(merged_df.columns)
        temp.append("weather_key")

        merged_df = pd.merge(left=merged_df,
                             right=dim_weather,
                             how='left',
                             left_on=baseweather_merge_cols,
                             right_on=weather_merge_cols)[temp]

        # 4. merged1 LJ dim_crime

        basecrime_merge_cols = ['IUCR', 'PrimaryType']
        crime_merge_cols = ['IUCR', 'PrimaryType']

        # Decide the columns we want
        temp = list(merged_df.columns)
        temp.append('IUCR_key')

        merged_df = pd.merge(left=merged_df,
                             right=dim_crime,
                             how='left',
                             left_on=basecrime_merge_cols,
                             right_on=crime_merge_cols)[temp]

        # Get only the relevant columns from the merged table
        fact_cols = ['ID', 'date_key', 'location_key', 'weather_key',
                     'IUCR_key',
                     'Arrest', 'Domestic', 'Latitude', 'Longitude', 'TempAvg']

        fact_crime = merged_df[fact_cols]

        return fact_crime

    def process_pipeline(self, data_frame):
        """
        Cleans the data frame
        :param data_frame: Pandas data frames
        :return: Dict of data frames where the key is the table name and the
        value is the dataframe of the table
        """

        # Remove all the rows that contain NaN values
        data_frame.dropna(inplace=True)

        # Transform the data

        # Make the date dim table
        dim_date = self._get_dim_date(data_frame)

        # Make the location dim table
        dim_location = self._get_dim_location(data_frame)

        # Make the crime dim table
        dim_crime = self._get_dim_crime(data_frame)

        # Make the weather dim table
        dim_weather = self._get_dim_weather(data_frame)

        # Merge the dimension tables with the base table to genrate the fact
        # table
        fact_crime = self._get_fact_table(data_frame,
                                          dim_date,
                                          dim_location,
                                          dim_crime,
                                          dim_weather)

        print(fact_crime.columns)

        # Return the dictionary of all members of the star schema
        star_tables = {
            "fact_crime": fact_crime,
            "dim_date": dim_date,
            "dim_weather": dim_weather,
            "dim_location": dim_location,
            "dim_crime": dim_crime,
        }
        return star_tables


if __name__ == "__main__":

    folder_path = "C:\\Users\\SSrih\\OneDrive\\UChicago\\DEP\\Project\\data" \
                  "\\Crime and Weather\\"
    # data_file_name = "CrimeWeather2010.csv"
    # data_file_name = "Crime2010Raw.csv"
    data_file_name = "Crime20161718.csv"

    data_file_path = os.path.join(folder_path, data_file_name)
    data_extractor = DataExtractor()
    data_frame = data_extractor.read_csv(fpath=data_file_path,
                                         nrows_to_read=5000)

    # print(data_frame.head())

    data_worker = DataWorker()

    print(data_frame.isnull().sum().sum())

    data_worker.process_pipeline(data_frame)

    print(data_frame.isnull().sum().sum())
