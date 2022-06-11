#
#  database/create/create.py
#  DataLake
#
#  Created by Clément Malonda
#

import os
import pandas as pd
from PIL import Image
import mysql.connector
from mysql.connector import errorcode

def create_classification_dataset(cursor: mysql.connector.cursor.MySQLCursor, data: dict):
    '''
        Function used to create a classification dataset
            Parameters:
                cursor (mysql.connector.cursor.MySQLCursor): SQL cursor used to send queries to the DB
                data (dict): dataset descriptor from json file
            Returns:
                None
    '''
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
        if res[i][2] != 2: # TEMP: condition will be remove
            df = df.append({"image": new_image_path, "label":res[i][1]}, ignore_index = True)
            img = Image.open(res[i][0])
        else:
            continue
        if res[i][2] == 0:
            img.save(os.path.join(data["path"], new_image_path), "JPEG")
        elif res[i][2] == 1:
            pos = res[i][3].split(";")
            print(pos)
            xmin = int(pos[0])
            ymin = int(pos[1])
            xmax = int(pos[2])
            ymax = int(pos[3])
            new_img = img.crop((xmin, ymin, xmax, ymax))
            new_img.save(os.path.join(data["path"], new_image_path), "JPEG")
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
