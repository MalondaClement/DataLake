# DataLake

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white)

-----

## 1. Database schema

![Database Schema](img/database_schema.png)

**Fig. 1** - _Database Schema_


## 2. How to use this project
### 2.1 Init the database

```
python3 main.py init
```

### 2.2 Insert data in the database
```
python3 main.py insert
```

### 2.3 Create a new dataset from the data in the database
```
python3 main.py create
```

The description of the new dataset is a json file :

```json
{
    "type": "[classif|detection|segmentation]",
    "path": "/path/to/the/root/directory",
    "classes": {
        "label_1": ["other_label_1", "other_label_1"],
        "label_2": [],
        "label_3": ["other_label_3"]
    }
}
```

### 2.4 List label names or datasets
```
python3 main.py list
```

### 2.5 Clear all the database
```
python3 main.py clear
```
