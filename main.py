import sys
import mysql.connector


def connect_to_database():
    cnx = mysql.connector.connect(user="root", password="bobafett", host="127.0.0.1", database="datalake")
    print(cnx)

def main():
    try :
        assert sys.argv[1] in ["init", "insert", "create", "list"]
    except AssertionError :
        print("First argument in the command line need to be : \n- insert, \n- create, \n- or list")
        exit(1)

    connect_to_database()

    if sys.argv[1] == "init" :
        pass
    elif sys.argv[1] == "insert" :
        pass
    elif sys.argv[1] == "create" :
        pass
    elif sys.argv[1] == "list" :
        pass

    cnx.close()

if __name__ == "__main__":
    main()
