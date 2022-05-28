import mysql.connector
from mysql.connector import errorcode
from getpass import getpass

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
