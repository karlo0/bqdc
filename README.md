## Jupyter notebooks and Python module to download/upload/synchronize metadata of Google BigQuery tables and corresponding Tags in Google Data Catalog on Google Cloud Platform (GCP)

# Overview
Python repository to download and upload metadata (Data Catalog Tags, BigQuery table, field descriptions and schemas) from BigQuery tables and its attached tags in Data Catalog. It can also synchronize table descriptions and field descriptions of tables from within BigQuery and constructs tags in Data Catalog for this metadata.

The repository contains the python module 'bqdc.py' and as frontend 3 Jupyter '.ipynb' scripts. Follow the specific instructions to be found in the individual Jupyter notebooks.

- 'download.ipynb':

  Use for downloading metadata from BigQuery and Data Catalog into an Excel .xlsx file, which is by default stored in a subfolder of this repository. Both the name of the folder and the Excel file are specified as the dataset ID string of the parent dataset of the downloaded tables in BigQuery.

- 'upload.ipynb':

  Use for uploading metadata from an Excel .xlsx file (e.g. obtained from a previous download which has since then been updated) to BigQuery and Data Catalog. Both the name of the folder and the file must be specified as the dataset ID string of the parent dataset in BigQuery of the to be uploaded tables.

- 'synchronize.ipynb':

  Use for synchronizing metadata between BigQuery and Data Catalog. If Data Catalog tags for tables exist that specify table and field descriptions but are not yet in BigQuery, these descriptions will be pushed to BigQuery.
  Contrary, when BigQuery table and field descriptions exist but no tag is in Data Catalog with this information, tags are created and attached to the table and/or its fields to specify this information too in Data Catalog.

# Requirements

The module can only be used when the following conditions are met:
- 2 tag templates in Data Catalog are **_required_**:
  - A tag template that is used to attach tags **directly** to BigQuery tables, in the following referred to as **table tag template**
  - A tag template that is used to attach tags to **fields** of BigQuery tables, in the following referred to as **field tag template**
- The **table tag template** is **_required_** to have an attribute with key name **'table_description'**, that is intended to store table descriptions similar to the attribute 'description' of the BigQuery 'Table' class
- The **field tag template** is **_required_** to have an attribute with key name **'field_description'**, that is intended to store field/column descriptions similar to the attribute 'description' of the BigQuery 'SchemaField' class

# Installation
1. Pull from GitHub
2. In main folder, execute `pip3 install -r requirements.txt` (Recommended to do this within a Python Virtual Environment. The requirement.txt should work with a python 3.6.8+ version )
3. Download for the correct service account the corresponding '.json' file containing the service account key and add it to the main folder of this repository
4. If you need to use backup metadata to upload to BigQuery and Data Catalog add Excel '.xlsx' files containing the metadata to the 'backup_sheets' folder of this repository. For each dataset there must be a separate backup '.xlsx' file that must be copied in a subfolder of the folder 'backup_sheets'. Both the subfolder name and the '.xlsx' backup file name must specify the dataset ID of the BigQuery dataset of which metadata for tables shall be uploaded.
5. Call `jupyter notebook` from a (Power)shell within the main folder of the repository and open one of the Jupyter scripts.
