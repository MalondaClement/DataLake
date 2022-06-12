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
#### 2.2.1 Insert classification dataset
Dataset tree:
* root
    * images
        * label_1
            * image1.jpg
            * image2.jpg
            * ...
        * label_2
            * image1.jpg
            * image2.jpg
            * ...
        * label_3
            * ...
    * labels.csv

labels.csv columns name `image`, `label`

#### 2.2.2 Insert detection dataset
##### 2.2.2.a XML format
Dataset tree:
* root
    * images
        * image1.jpg
        * image2.jpg
        * ...
    * labels
        * label1.xlm
        * label2.xlm
        * ...

```xml
<annotation>
	<filename>000005.jpg</filename>
	<size>
		<width>500</width>
		<height>375</height>
		<depth>3</depth>
	</size>
	<object>
		<name>chair</name>
		<bndbox>
			<xmin>263</xmin>
			<ymin>211</ymin>
			<xmax>324</xmax>
			<ymax>339</ymax>
		</bndbox>
	</object>
	<object>
		<name>chair</name>
		<bndbox>
			<xmin>165</xmin>
			<ymin>264</ymin>
			<xmax>253</xmax>
			<ymax>372</ymax>
		</bndbox>
	</object>
	<object>
		<name>chair</name>
		<bndbox>
			<xmin>5</xmin>
			<ymin>244</ymin>
			<xmax>67</xmax>
			<ymax>374</ymax>
		</bndbox>
	</object>
	<object>
		<name>chair</name>
		<bndbox>
			<xmin>241</xmin>
			<ymin>194</ymin>
			<xmax>295</xmax>
			<ymax>299</ymax>
		</bndbox>
	</object>
	<object>
		<name>chair</name>
		<bndbox>
			<xmin>277</xmin>
			<ymin>186</ymin>
			<xmax>312</xmax>
			<ymax>220</ymax>
		</bndbox>
	</object>
</annotation>
```

##### 2.2.2.b CSV format
Dataset tree:
* root
    * images
        * image1.jpg
        * image2.jpg
        * ...
    * labels.csv

labels.csv columns name `image`, `label`, `xmin`, `ymin`, `xmax`, `ymax`

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
