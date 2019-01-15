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

import time
from Receiver import Receiver
from DataLoader import DataLoader


EX_NAME = "ping"
SUB_TOPIC = "star_update"

DATA_HUB_IP = "35.231.210.202"
# DATA_HUB_IP = "localhost"
DATA_HUB_UNAME = "admin"
DATA_HUB_PWD = "password"

STAR_DB_IP = "35.202.176.53"
STAR_DB_USERNAME = "srihari"
STAR_DB_PASSWORD = "depdinos123"

LOCAL_DB_USERNAME = "root"
LOCAL_DB_PASSWORD = "root"


def cludge_work():
    """
    Downloads the dataset from the cloud onto the local MySQL instance
    :return:
    """

    total_start_time = time.time()

    # Make a connection to the star database
    database = 'crime_star'
    user = 'srihari'
    password = 'depdinos123'
    port = '3306'

    star_data_loader = DataLoader()

    ret = star_data_loader.connect(host=STAR_DB_IP,
                                   database=database,
                                   user=user,
                                   password=password,
                                   port=port)

    if ret != 1:
        print(" Connection not established. Try again")
        print(" Check internet connectivity")
        return ret

    # Make a connection to the local database
    database = 'crime_star'
    port = '3306'

    local_loader = DataLoader()

    ret = local_loader.connect(host='localhost',
                               database=database,
                               user=LOCAL_DB_USERNAME,
                               password=LOCAL_DB_PASSWORD,
                               port=port)

    if ret != 1:
        print(" Connection not established. Try again")
        print(" Check internet connectivity")
        return ret

    # Pull the data from the databse and push it into the local DB for analysis
    table_list = star_data_loader.get_tables()

    star_tables = {}
    for table in table_list:
        query_str = "SELECT * FROM " + table + ";"
        print("\tmonitoring table :", table, "of size", end=" ")
        table_data = star_data_loader.execute_query(query=query_str)
        print(table_data.shape)
        star_tables[table] = table_data

    local_loader.load_star(star_tables=star_tables, if_table_exists="append")

    star_data_loader.disconnect()
    local_loader.disconnect()

    total_end_time = time.time()

    print(" Total time taken :", total_end_time - total_start_time)


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

    try:
        content_str = body.decode("utf-8")

        print("\n Received : ", content_str, "\n")

        cludge_work()

    except Exception as e:
        print("Exception caught in callback ")
        print(e)


def main():

    # ------------------------------------------------------------------------ #
    # 0. Subscribe to the broker
    # ------------------------------------------------------------------------ #

    receiver = Receiver()

    # Connect to the data hub
    uname = "admin"
    password = "password"
    receiver.connect(host=DATA_HUB_IP, uname=uname, pwd=password)

    # Connect to the exchange
    receiver.get_data_from_exchange(ex_name=EX_NAME,
                                    topic=SUB_TOPIC,
                                    callback=process_data)


if __name__ == "__main__":
    main()
