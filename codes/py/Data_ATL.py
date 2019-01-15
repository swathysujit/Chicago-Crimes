"""
@file_name : Data_ATL.py
@author : Srihari Seshadri
@description : This file does the following operations :
                1. Connects to the broker and listens for updates for database
                2. Reads the input data in csv format
                3. Connects with the database
                4. Performs CRUD operations as required
@date : 11-13-2018
"""

import os
import time
from SQLDatabaseManager import SQLDatabaseManager
from DataWorker import DataWorker
from DataLoader import DataLoader
from Messenger import Messenger
from Receiver import Receiver


EX_NAME = "ping"
SUB_TOPIC = "update"
PUB_TOPIC = "star_update"
RAW_TABLE_NAME = "crime_weather_2017"

DATA_HUB_IP = "35.231.210.202"
# DATA_HUB_IP = "localhost"
DATA_HUB_UNAME = "admin"
DATA_HUB_PWD = "password"

STAR_DB_IP = "35.202.176.53"
STAR_DB_USERNAME = "srihari"
STAR_DB_PASSWORD = "depdinos123"


def process_data(ch, method, properties, body):
    """
    Function establishes connection with the database, pulls the data,
    transforms it and loads it into the analysis database
    :param ch: Channel
    :param method: Method
    :param properties: Properties
    :param body: content of the message
    :return: 1 if success, -1 if failure
    """

    total_start_time = time.time()

    try:
        content_str = body.decode("utf-8")

        print("Received : ", content_str)

        # -------------------------------------------------------------------- #
        # 1. ESTABLISH DATABASE CONNECTION WITH THE RAW DATA BASE AND THE STAR
        # DATABASE
        # -------------------------------------------------------------------- #

        print("\n\n\t\t **** 1. DATABASE CONNECTIONS **** ")

        # host = 'localhost'
        # database = 'crime_star'
        # user = 'root'
        # password = 'root'
        # port = '3306'

        host = '35.224.172.74'
        database = 'crime_weather_raw'
        user = 'srihari'
        password = 'depdinos123'
        port = '3306'

        raw_data_loader = DataLoader()

        ret = raw_data_loader.connect(host=host,
                                      database=database,
                                      user=user,
                                      password=password,
                                      port=port)

        if ret != 1:
            print(" Connection not established. Try again")
            print(" Check internet connectivity")
            return ret

        # Now with the analysis database server for the star schema

        database = 'crime_star'
        port = '3306'

        star_data_loader = DataLoader()

        ret = star_data_loader.connect(host=STAR_DB_IP,
                                       database=database,
                                       user=STAR_DB_USERNAME,
                                       password=STAR_DB_PASSWORD,
                                       port=port)

        if ret != 1:
            print(" Connection not established. Try again")
            print(" Check internet connectivity")
            return ret

        # -------------------------------------------------------------------- #
        # 2. PULL THE RAW DATA AND TRANSFORM IT
        # -------------------------------------------------------------------- #

        query_str = "SELECT * FROM " + RAW_TABLE_NAME + ";"
        data_frame = raw_data_loader.execute_query(query=query_str)

        # Disconnect the database
        raw_data_loader.disconnect()

        print(data_frame.shape)

        print("\n\n\t\t **** 2. DATA TRANSFORMATION **** ")

        data_worker = DataWorker()

        star_tables = data_worker.process_pipeline(data_frame)

        # -------------------------------------------------------------------- #
        # 5. DATA LOADING PHASE
        # -------------------------------------------------------------------- #

        print("\n\n\t\t **** 2. DATA LOADING **** ")
        ret = star_data_loader.load_star(star_tables)

        if ret == -1:
            print(" Could not upload to database ")
            return

        print("Successfully populated database")

        # -------------------------------------------------------------------- #
        # 6. DISCONNECT THE DATABASE AND CLEAN UP MEMORY
        # -------------------------------------------------------------------- #

        star_data_loader.disconnect()

        # -------------------------------------------------------------------- #
        # 7. SEND A MESSAGE TO THE DATA HUB AS AN UPDATE
        # -------------------------------------------------------------------- #

        print(" Sending message to data hub for update....", end="")

        messenger = Messenger()
        # Connect to the data hub
        messenger.connect(host=DATA_HUB_IP,
                          uname=DATA_HUB_UNAME,
                          pwd=DATA_HUB_PWD)

        # Connect to the exchange
        messenger.connect_to_exchange(ex_name=EX_NAME)

        # Send update
        message = "Star schema created. Have fun!"
        messenger.send_message_to_exchange(ex_name=EX_NAME,
                                           message=message,
                                           topic=PUB_TOPIC)

        print("sent")

    except Exception as e:
        print("Exception caught in callback ")
        print(e)

    total_end_time = time.time()

    print(" Total time taken :", total_end_time - total_start_time)


def main():

    # ------------------------------------------------------------------------ #
    # 0. Subscribe to the broker
    # ------------------------------------------------------------------------ #

    receiver = Receiver()

    # Connect to the data hub
    receiver.connect(host=DATA_HUB_IP,
                     uname=DATA_HUB_UNAME,
                     pwd=DATA_HUB_PWD)

    # Connect to the exchange
    receiver.get_data_from_exchange(ex_name=EX_NAME,
                                    topic=SUB_TOPIC,
                                    callback=process_data)


if __name__ == "__main__":
    main()
