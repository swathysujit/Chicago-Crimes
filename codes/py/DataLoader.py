"""
@file_name : DataLoader.py
@author : Srihari Seshadri
@description : This file loads the tables into the database based on preset
                configurations
@date : 12-05-2018
"""

from SQLDatabaseManager import SQLDatabaseManager


class DataLoader(SQLDatabaseManager):

    def __init__(self):
        self._sqldbm = None
        super(DataLoader, self).__init__()

    def connect(self, host, database, user, password, port):
        self._sqldbm = SQLDatabaseManager()

        ret = self._sqldbm.connect(host=host,
                                   database=database,
                                   username=user,
                                   password=password,
                                   port=port)

        if ret != 1:
            print(" Closing program ")
            return ret

        # print(self._sqldbm.get_tables())

        return ret

    def execute_query(self, query):
        return self._sqldbm.execute_query(query=query)

    def load_star(self, star_tables, if_table_exists="replace"):

        # First remove safe update mode.
        query = "SET SQL_SAFE_UPDATES = 0;"
        self._sqldbm.execute_query(query=query)

        for k, v in star_tables.items():

            print(" Cleaning table : ", k)

            query = "DELETE FROM " + k + ";"
            self._sqldbm.execute_query(query)

            # Any customisation?
            if "date" in k:
                v = v.rename(index=str, columns={"Date": "datetime"})
            elif "weather" in k:
                v = v.rename(index=str, columns={"Tmin": "TempMin"})

            print(" \tUpdating with new values....", end="")
            ret = self._sqldbm.insert(dframe=v,
                                      table_name=k,
                                      if_table_exists=if_table_exists)
            print("done.")

            if ret != 1:
                print(" Could not insert into database. Check connectivity")
                continue

        # Re-enable safe update mode.
        query = "SET SQL_SAFE_UPDATES = 1;"
        self._sqldbm.execute_query(query=query)

        return 1

    def load_full_table(self, data_frame, table_name,
                        if_table_exists="replace"):
        ret = self._sqldbm.insert(dframe=data_frame,
                                  table_name=table_name,
                                  if_table_exists=if_table_exists)

        if ret != 1:
            print(" Could not insert into database. Check connectivity")
        return ret

    def disconnect(self):
        self._sqldbm.disconnect()
        print(" Disconnected from database ")

    def get_tables(self):
        return self._sqldbm.get_tables()
