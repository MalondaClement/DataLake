import mysql.connector
from mysql.connector import errorcode

def connect_to_database():
    user = input("User : ")
    password = input("Password : ")
    try:
        cnx = mysql.connector.connect(user=user, password=password, host="127.0.0.1")#, database="datalake")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        exit(1)
    return cnx

def init_database():
    pass
