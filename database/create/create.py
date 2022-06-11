#
#  database/create/create.py
#  DataLake
#
#  Created by Clément Malonda
#

import os
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

def create_classification_dataset(cursor: mysql.connector.cursor.MySQLCursor, data: dict):
    res = list()
    os.mkdir(os.path.join(data["path"], "images"))
    for c in list(data["classes"].keys()):
        os.mkdir(os.path.join(data["path"], "images", c))
        __execute_query(cursor, c)
        for i in cursor:
            res.append(list(i))

        for c_substitute in data["classes"][c]:
            __execute_query(cursor, c_substitute)
            for i in cursor:
                tmp = list(i)
                tmp[1] = c
                res.append(tmp)

    df = pd.DataFrame(columns=["image","label"])
    for i in range(len(res)):
        new_image_path = os.path.join("images", res[i][1],str(i)) + ".jpg"
        df = df.append({"image": new_image_path, "label":res[i][1]}, ignore_index = True)
    print(df)
    df.to_csv(os.path.join(data["path"], "labels.csv"), index=False)


def create_detection_dataset(cursor, data):
    pass

def create_segmentation_dataset(cursor, data):
    pass

def __execute_query(cursor: mysql.connector.cursor.MySQLCursor, className: str):
    try:
        cursor.execute("SELECT path, className, labelType, points FROM `datalake`.`label` WHERE className=\"{}\";".format(className))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_TABLE_ERROR:
            print("The table not exists")
        else:
            print(err.msg)
