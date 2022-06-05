#
#  database/insertion/classif.py
#  DataLake
#
#  Created by Clément Malonda
#

import mysql.connector
from mysql.connector import errorcode

def insert_dataset(cursor: mysql.connector.cursor.MySQLCursor, name: str, type: int) -> bool:
    try:
        cursor.execute("INSERT INTO datalake.dataset (name, datasetType) VALUES (\"{}\", {});".format(name, type))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            print("The dataset {} is already in the database".format(name))
        elif err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Print the table Dataset does not exist")
            print("Please use \"python3 main.py init\" before")
        else:
            print("Unexpected error {}".format(err.errno))
        return False
    return True
