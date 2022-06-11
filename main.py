#
#  main.py
#  DataLake
#
#  Created by Clément Malonda
#

import sys

from database import connect_to_database, init_database, clear_database, list_database, insert_data, create_dataset

OPTIONS = ["init", "insert", "create", "list", "clear"]

def main():
    if len(sys.argv) < 2:
        print("The command line needs at least one argument in the following list: {}".format(OPTIONS))
        exit(1)
    try:
        assert sys.argv[1] in OPTIONS
    except AssertionError or IndexError:
        print("First argument in the command line needs to be in following: {}".format(OPTIONS))
        exit(1)

    cnx = connect_to_database()
    cursor = cnx.cursor()

    if sys.argv[1] == "init":
        init_database(cursor)
    elif sys.argv[1] == "insert":
        insert_data(cnx, cursor)
    elif sys.argv[1] == "create":
        create_dataset(cursor)
    elif sys.argv[1] == "list":
        list_database(cursor)
    elif sys.argv[1] == "clear":
        clear_database(cursor)

    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
