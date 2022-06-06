#
#  database/database.py
#  DataLake
#
#  Created by Clément Malonda
#

import os
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from getpass import getpass
from database.insertion import insert_dataset, insert_class, insert_image, insert_label

LIST_OPTIONS = ["dataset", "class"]

INSERT_OPTIONS = ["classif", "detection", "segmentation"]

TABLES = dict()

TABLES["dataset"] = (
    "CREATE TABLE `dataset` ("
    "   `name` varchar(20) NOT NULL,"
    "   `datasetType` int NOT NULL,"
    "   PRIMARY KEY (`name`)"
    ")"
)

TABLES["class"] = (
    "CREATE TABLE `class` ("
    "   `className` varchar(20) NOT NULL,"
    "   `classID` int NOT NULL,"
    "   PRIMARY KEY (`className`)"
    ")"
)

TABLES["image"] = (
    "CREATE TABLE `image` ("
    "   `path` varchar(200) NOT NULL,"
    "   `dataset` varchar(20) NOT NULL,"
    "   `height` int NOT NULL,"
    "   `width` int NOT NULL,"
    "   `imageType` varchar(5),"
    "   PRIMARY KEY (`path`),"
    "   CONSTRAINT `image_fk` FOREIGN KEY (`dataset`) "
    "       REFERENCES `dataset` (`name`)"
    ")"
)

TABLES["label"] = (
    "CREATE TABLE `label` ("
    "   `labelID` int NOT NULL,"
    "   `path` varchar(200) NOT NULL,"
    "   `className` varchar(20) NOT NULL,"
    "   `labelType` int NOT NULL,"
    "   `points` varchar(200) NOT NULL,"
    "   PRIMARY KEY (`labelID`),"
    "   CONSTRAINT `label_fk_1` FOREIGN KEY (`path`) "
    "       REFERENCES `image` (`path`),"
    "   CONSTRAINT `label_fk_2` FOREIGN KEY (`className`) "
    "       REFERENCES `class` (`className`)"
    ")"
)

def connect_to_database() -> mysql.connector.connection.MySQLConnection:
    '''
        Connection to MySQL database "datalake" using localhost
        Asks for a user and a password
            Returns:
                cnx (mysql.connector.connection.MySQLConnection): connection object
    '''
    user = input("User: ")
    password = getpass()
    try:
        cnx = mysql.connector.connect(user=user, password=password, host="127.0.0.1", database="datalake")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        exit(1)
    else:
        print("Connection OK")
    return cnx

def init_database(cursor: mysql.connector.cursor.MySQLCursor):
    '''
        Function used to initialize all the tables in the database
            Parameters:
                cursor (mysql.connector.cursor.MySQLCursor): SQL cursor used to send queries to the DB
            Returns:
                None
    '''
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creatting table {}: ".format(table_name), end="")
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists")
            else:
                print(err.msg)
        else:
            print("OK")

def clear_database(cursor: mysql.connector.cursor.MySQLCursor):
    '''
        Function used to clear the DB
            Parameters:
                cursor (mysql.connector.cursor.MySQLCursor): SQL cursor used to send queries to the DB
            Returns:
                None
    '''
    queries = {
        "label": "DROP TABLE `datalake`.`label`;",
        "image": "DROP TABLE `datalake`.`image`;",
        "dataset": "DROP TABLE `datalake`.`dataset`;",
        "class": "DROP TABLE `datalake`.`class`;"
    }
    # query = "DROP TABLE `datalake`.`class`, `datalake`.`dataset`, `datalake`.`image`, `datalake`.`label`;"
    for table_name in queries:
        query = queries[table_name]
        try:
            print("Dropping table {}: ".format(table_name), end="")
            cursor.execute(query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print("not exists")
            else:
                print(err.msg)
        else:
            print("OK")

def list_database(cursor: mysql.connector.cursor.MySQLCursor):
    '''
        Function used to list some elements in the database
            Parameters:
                cursor (mysql.connector.cursor.MySQLCursor): SQL cursor used to send queries to the DB
            Returns:
                None
    '''
    print("Options available: {}".format(LIST_OPTIONS))
    option = ""
    while option not in LIST_OPTIONS :
        option = input("Select an option: ")

    if option == LIST_OPTIONS[0]:
        try:
            cursor.execute("SELECT * FROM `datalake`.`dataset`;")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print("The table not exists")
            else:
                print(err.msg)
    elif option == LIST_OPTIONS[1]:
        try:
            cursor.execute("SELECT * FROM `datalake`.`class`;")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print("The table not exists")
            else:
                print(err.msg)
    for i in cursor:
        print(i)

def insert_data(cursor: mysql.connector.cursor.MySQLCursor):
    '''
        Function used to clear the DB
            Parameters:
                cursor (mysql.connector.cursor.MySQLCursor): SQL cursor used to send queries to the DB
            Returns:
                None
    '''
    print("Options available: {}".format(INSERT_OPTIONS))
    option = ""
    while option not in INSERT_OPTIONS :
        option = input("Select an option: ")
    name = input("Dataset name: ")
    path = input("Dataset path: ")
    if not os.path.isdir(path):
        print("{} is not a directory".format(path))
        return

    if option == INSERT_OPTIONS[0]:
        if not insert_dataset(cursor, name, 0):
            return
        try:
            labels = pd.read_csv(os.path.join(path, "labels.csv"))
        except FileNotFoundError:
            print("The file {} does not exist".format(os.path.join(path, "labels.csv")))
            return 1
        for i in labels.index:
            # print("Insert {} {}".format(os.path.join(path,labels["image"][i]), labels["label"][i]))
            # inserer class
            insert_class(cursor, labels["label"][i])
            insert_image(cursor, os.path.join(path, labels["image"][i]), name)
            # inserer label

    elif option == INSERT_OPTIONS[1]:
        if not insert_dataset(cursor, name, 1):
            return
    elif option == INSERT_OPTIONS[2]:
        if not insert_dataset(cursor, name, 2):
            return
