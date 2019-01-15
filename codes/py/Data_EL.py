"""
@file_name : Data_EL.py
@author : Srihari Seshadri
@description : This file does the following operations :
                1. Reads the configuration file for getting all info required
                2. Reads the input data in csv format
                3. Connects with the database
                4. Performs CRUD operations as required
@date : 11-13-2018
"""

import os
import time
from DataExtractor import DataExtractor
from DataWorker import DataWorker
from DataLoader import DataLoader
from Messenger import Messenger

FOLDER_PATH = "C:\\Users\\SSrih\\OneDrive\\UChicago\\DEP\\Project\\data" \
              "\\Crime and Weather\\"

EX_NAME = "ping"
TOPIC = "update"
RAW_TABLE_NAME = "crime_weather_2017"

DATA_HUB_IP = "35.231.210.202"
# DATA_HUB_IP = "localhost"
DATA_HUB_UNAME = "admin"
DATA_HUB_PWD = "password"

DB_IP = '35.224.172.74'
DB = 'crime_weather_raw'
DB_UNAME = 'srihari'
DB_PWD = 'depdinos123'


def main():

    total_start_time = time.time()

    # ------------------------------------------------------------------------ #
    # 0. PARSE INPUT ARGUMENTS
    # ------------------------------------------------------------------------ #

    data_file_name = "Crime_Weather_Cleaned_2017.csv"
    # data_file_name = "Crime20161718.csv"

    data_file_path = os.path.join(FOLDER_PATH, data_file_name)

    # ------------------------------------------------------------------------ #
    # 1. ESTABLISH DATABASE CONNECTION
    # ------------------------------------------------------------------------ #

    print("\n\n\t\t **** 1. DATABASE CONNECTION **** ")

    # host = 'localhost'
    # database = 'crime_star'
    # user = 'root'
    # password = 'root'
    # port = '3306'

    port = '3306'

    data_loader = DataLoader()

    ret = data_loader.connect(host=DB_IP,
                              database=DB,
                              user=DB_UNAME,
                              password=DB_PWD,
                              port=port)

    if ret != 1:
        print(" Connection not established. Try again")
        print(" Check internet connectivity")
        return ret

    # ------------------------------------------------------------------------ #
    # 2. DATA EXTRACTION PHASE
    # ------------------------------------------------------------------------ #

    print("\n\n\t\t **** 2. DATA EXTRACTION **** ")

    data_extractor = DataExtractor()
    data_frame = data_extractor.read_csv(fpath=data_file_path,
                                         nrows_to_read=-1)

    # ------------------------------------------------------------------------ #
    # 3. DATA LOADING PHASE
    # ------------------------------------------------------------------------ #

    print("\n\n\t\t **** 2. DATA LOADING **** ")

    ret = data_loader.load_full_table(data_frame,
                                      table_name=RAW_TABLE_NAME)

    if ret == -1:
        print(" Could not upload to database ")
        data_loader.disconnect()
        return

    print("Successfully populated database")

    # ------------------------------------------------------------------------ #
    # 4. DISCONNECT THE DATABASE AND CLEAN UP MEMORY
    # ------------------------------------------------------------------------ #

    data_loader.disconnect()

    # ------------------------------------------------------------------------ #
    # 5. SEND A MESSAGE TO THE DATA HUB AS AN UPDATE
    # ------------------------------------------------------------------------ #

    print(" Sending message to data hub for update....", end="")

    messenger = Messenger()
    # Connect to the data hub
    messenger.connect(host=DATA_HUB_IP,
                      uname=DATA_HUB_UNAME,
                      pwd=DATA_HUB_PWD)

    # Connect to the exchange
    messenger.connect_to_exchange(ex_name=EX_NAME)

    # Send update
    message = "Database updated with latest rows"
    messenger.send_message_to_exchange(ex_name=EX_NAME,
                                       message=message,
                                       topic=TOPIC)

    print("sent")

    total_end_time = time.time()

    print(" Total time taken :", total_end_time - total_start_time)


if __name__ == "__main__":
    main()
