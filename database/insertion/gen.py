#
#  database/insertion/gen.py
#  DataLake
#
#  Created by Clément Malonda
#

import time
from PIL import Image
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

def insert_class(cursor: mysql.connector.cursor.MySQLCursor, name: str) -> bool:
    try:
        cursor.execute("INSERT INTO datalake.class (className, classID) VALUES (\"{}\", {});".format(name, time.time()))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            return True
        elif err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Print the table Dataset does not exist")
            print("Please use \"python3 main.py init\" before")
        else:
            print("Unexpected error {}".format(err.errno))
        return False
    return True

def insert_image(cursor: mysql.connector.cursor.MySQLCursor, path: str, dataset_name: str) -> bool:
    im = Image.open(path)
    im_width, im_height = im.size
    del im
    if path[-3:] == "jpg" or path[-3:] == "jpeg":
        im_type = "jpg"
    elif path[-3:] == "png":
        im_type = "png"
    else:
        im_type = "none"
    try:
        cursor.execute("INSERT INTO datalake.image (path, dataset, height, width, imageType) VALUES (\"{}\", \"{}\", {}, {}, \"{}\");".format(path, dataset_name, im_height, im_width, im_type))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_DUP_ENTRY:
            return True
        elif err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("Print the table Dataset does not exist")
            print("Please use \"python3 main.py init\" before")
        else:
            print("Unexpected error {}".format(err.errno))
        return False
    return True

def insert_label():
    pass
