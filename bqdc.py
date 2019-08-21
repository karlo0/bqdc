r"""
bqdc.py

    Python Module to download, upload metadata (Datacatalog Tags, BigQuery table, field descriptions and schema) from Bigquery Tables and its attached tags in DataCatalog.
    It can synchronize table descriptions and field descriptions from within Bigquery and constructs tags in Datacatalog for this metadata.
    The main funcions are the following:

    - The download function stores metadata in an Excel .xlsx file
    - The upload function uses the metadata from an Excel .xlsx file (e.g. obtained from a previous download which has since then been updated) and uploads it to BigQuery and DataCatalog
    - The synchronize function downloads the metadata and uploads it again to BigQuery and DataCatalog. This can be used to synchronize table and field description metadata that is found in
    one of the two GCP apps to upload it to the other too, if it has not been there before

    Please check the jupyter notebooks for more detailed information.

    The module can only be used when the following conditions are met:
        - 2 tag templates in DataCatalog are specified:
            - A tag template that is used to attach tags to whole BigQuery tables, in the following referred to as table tag template
            - A tag template that is used to attach tags to fields of BigQuery tables, in the following referred to as field tag template
        - The table tag template is required to have an attribute with key name 'table_description', that is intended to store table descriptions similar to the attribute 'description' of the BigQuery 'Table' class
        - The field tag template is required to have an attribute with key name 'field_description', that is intended to store field/column descriptions similar to the attribute 'description' of the BigQuery 'SchemaField' class

Version: 0.1
Author: Karsten Leonhardt
Date: 21.08.2019
"""

# Connect to the Google Data Catalog python modul
from google.cloud import datacatalog_v1beta1
# Connect to the Google BigQuery python modul
from google.cloud import bigquery
# Connect to the Google Authentification python modul
from google.oauth2 import service_account
import pandas as pd
from collections import OrderedDict
import os
import shutil
import re
import glob
from functools import reduce

class clients:

    type_table_ref_bq = bigquery.TableReference
    # BigQuery maximum string length
    bq_max_string_length = 1024
    # DataCatalog maximum string length
    dc_max_string_length = 2000
    # the current path
    CDIR = os.path.dirname(os.path.realpath(__file__))

    def __init__(self, PROJECT_ID, PATH_SERVICE_ACCOUNT_KEY):

        self.P_ID = PROJECT_ID
        self.DS_ID = None

        """get credentials through service account file"""
        self.credentials = service_account.Credentials.from_service_account_file(PATH_SERVICE_ACCOUNT_KEY)

        """establish a datacatalog client"""
        self.dc_client = datacatalog_v1beta1.DataCatalogClient(credentials=self.credentials)

        """establish a BigQuery client"""
        self.bq_client = bigquery.Client(project=PROJECT_ID, credentials = self.credentials)

class toolbox(clients):

    pattern_table_descr_bq_pure = re.compile(r"(?P<descr>^[\s\S]*?)\s*Table attributes")
    # max length of sheet names in Excel
    excel_max_sheet_name_length = 31
    overview_sheet_name = 'metadata_of_tables'


    def __init__(self, PROJECT_ID, PATH_SERVICE_ACCOUNT_KEY = None, prefer_bq_for_downlad_update = True, logfile = '', do_print_log = False):
        """
        This class establishes a connection to both Bigquery and Datacatalog clients and allows for the
        manipulation and creation of tags in Datacatalog attached to Bigquery tables and the manipulation of BigQuery table schemas.

        Parameters:
        -----------
        PROJECT_ID: String
            Specifies the GCP Project ID of which resources in BigQuery and Datacatalog are requested.

        PATH_SERVICE_ACCOUNT_KEY: String, None (Default)
            The full path to the Json file containing the service account key. If no string is provided, it searches for a .json file in the current directory and tries to connect to the BQ and DC clients with this file.

        prefer_bq_for_download_update: False, True (Default)
            When set to true, the table description of BQ is prefered over the DC table description when it exists.

        logfile: String, '' (Default)
            When the specified string is not empty it will created in the current directory a logfile with the specified string as name. If not provided, no logfile is written

        do_print_log: True, False (Default)
            if 'True' print log status messages to the stdout aka the screen

        Return:
        -------
        Instance of class 'toolbox'
        """

        assert isinstance(PROJECT_ID, str), "The 'PROJECT_ID' argument requires a string to specify the project ID to the GCP project for which BigQuery and DataCatalog resources are requested."

        if PATH_SERVICE_ACCOUNT_KEY is None:
            service_key_list = glob.glob('*.json')
            if len(service_key_list) == 1:
                PATH_SERVICE_ACCOUNT_KEY = os.path.join(self.CDIR, service_key_list[0])
            elif len(service_key_list) == 0:
                raise Exception("No service account key found in the current folder. Please initialise the object with the 'PATH_SERVICE_ACCOUNT_KEY' argument set to the full path (including the json filename with .json extension) of the service account key")
            else:
                raise Exception("There are more than one .json files in the current folder. Please initialise the object with the 'PATH_SERVICE_ACCOUNT_KEY' argument set to the full path (including the json filename with .json extension) of the service account key")

        super().__init__(PROJECT_ID, PATH_SERVICE_ACCOUNT_KEY)

        self.sheet = None
        self.ds_table_tags_df = None
        self.ds_field_tags_dicts = None
        self.table_instance_dc = None
        self.table_instance_bq = None
        self.__table_id = None
        self.__table_id_dc = ''
        self.__prefer_bq_for_downlad_update = prefer_bq_for_downlad_update

        self.__update = False
        self.__do_print_log = do_print_log

        if len(logfile) > 0:
            self.__do_log = True
            self.__log = ''
            self.__logfile = logfile
        else:
            self.__do_log = False

    def init_tag_templates(self, table_tag_template_str = None, field_tag_template_str = None, LOCATION_TAGS = 'us-central1', table_tag_fields_keys_ordered = [], field_tag_fields_keys_ordered = []):
        """
        Initializes tag templates. The whole class requires 2 tag templates:
            - a tag template whose id is specified by the 'table_tag_template_str' argument and which is used to attach tags to tables themselves
            - a tag template whose id is specified by the 'field_tag_template_str' argument and which is used to attach tags to fields of tables

        table_tag_template_str: String, None (Default)
            specifies an ID of a tag template that is used to attach tags to tables. The default is None, however the initialisation fails when no string is provided

        field_tag_template_str: String, None (Default)
            specifies an ID of a tag template that is used to attach tags to fields of tables. The default is None, however the initialisation fails when no string is provided

        LOCATION_TAGS: String, 'us-central1' (Default)
            The location of the tags. At the moment only 'us-central1' is supported

        table_tag_fields_keys_ordered: List of Strings, Empty lis (Default)
            A list of the table tag template attribute keys ordered in a list. If this is not provided the internal ordering of the attribute keys is used to set up DataFrame columns

        field_tag_fields_keys_ordered: List of Strings, Empty lis (Default)
            A list of the field tag template attribute keys ordered in a list. If this is not provided the internal ordering of the attribute keys is used to set up DataFrame columns
        """
        assert isinstance(table_tag_template_str, str), "A string must be passed for the 'table_tag_template_str' argument to specify an ID of a tag template that is used to attach tags to tables"
        assert isinstance(field_tag_template_str, str), "A string must be passed for the 'field_tag_template_str' argument to specify an ID of a tag template that is used to attach tags to fields of tables"

        self.TABLE_TAG_TEMPLATE_STR = table_tag_template_str
        self.FIELD_TAG_TEMPLATE_STR = field_tag_template_str

        TABLE_TAG_TEMPLATE_PATH=self.dc_client.tag_template_path(self.P_ID, LOCATION_TAGS, self.TABLE_TAG_TEMPLATE_STR)
        FIELD_TAG_TEMPLATE_PATH=self.dc_client.tag_template_path(self.P_ID, LOCATION_TAGS, self.FIELD_TAG_TEMPLATE_STR)
        
        try:
            self.table_tag_template = self.dc_client.get_tag_template(TABLE_TAG_TEMPLATE_PATH)
        except:
            msg = "Referencing the tag template used for attaching tags to whole tables failed"
            raise Exception(msg)

        try:
            self.field_tag_template = self.dc_client.get_tag_template(FIELD_TAG_TEMPLATE_PATH)
        except:
            msg = "Referencing the tag template used for attaching tags to fields of tables failed"
            raise Exception(msg)

        assert 'table_description' in self.table_tag_template.fields.keys(), "The tag template used for attaching tags to whole tables must contain an attribute with key ID = 'table_description'"
        assert 'field_description' in self.field_tag_template.fields.keys(), "The tag template used for attaching tags to fields of tables must contain an attribute with key ID = 'field_description'"

        self.__table_tag_fields_keys_ordered = self.__check_complete_ordered_list_of_keys(table_tag_fields_keys_ordered, self.table_tag_template.fields.keys())
        self.__field_tag_fields_keys_ordered = self.__check_complete_ordered_list_of_keys(field_tag_fields_keys_ordered, self.field_tag_template.fields.keys())

        self.__field_keys_to_ndx = {field_keys: k for k, field_keys in enumerate(self.__field_tag_fields_keys_ordered)}


        self.__upload_table_description_bq_init()
        pass

    def __check_complete_ordered_list_of_keys(self, ordered_keys_in, keys):
        set_keys_intersect = set(ordered_keys_in).intersection(set(keys))
        set_remaining_keys = set(keys) - set_keys_intersect

        ordered_keys = [key for key in ordered_keys_in if key in set_keys_intersect]

        for key in set_remaining_keys:
            ordered_keys.append(key)

        return ordered_keys


    def set_dataset(self, DS_ID):
        self.DS_ID = DS_ID
        pass

    def get_table_instance_dc(self, table_id, return_instance = False):
        resource_name = "//bigquery.googleapis.com/projects/{}/datasets/{}/tables/{}".format(self.P_ID, self.DS_ID, table_id)
        self.table_instance_dc = self.dc_client.lookup_entry(linked_resource=resource_name)
        if self.__table_id != table_id:
            self.__table_id = table_id
        if return_instance:
            return self.table_instance_dc
        else:
            pass

    def get_table_instance_bq(self, table_x, return_instance = False):
        if(isinstance(table_x, self.type_table_ref_bq)):
            self.table_instance_bq = self.bq_client.get_table(table_x)
        elif(isinstance(table_x, str)):
            try:
                self.table_instance_bq = self.bq_client.get_table(self.P_ID+'.'+self.DS_ID+'.'+table_x)
            except:
                raise Exception('The table can not be found under the specified PROJECT_ID/DATASET_ID')
                pass
        else:
            raise Exception('String or table_reference required as argument')

        if self.__table_id != self.table_instance_bq.table_id:
            self.__table_id = self.table_instance_bq.table_id

        self.get_bq_schema_metadata()
        if return_instance:
            return self.table_instance_bq
        else:
            pass

    def list_all_tags_entry(self, entry = None):
        """
        Prints all the tags attached to an entry (here an entry is a table instance)
        """
        if entry is None:
            entry = self.table_instance_dc

        if entry is not None:
            for tag in self.dc_client.list_tags(entry.name):
                print(tag)
        else:
            raise Exception('\nNo datacatalog entry instance provided. Call method again as ''list_all_tags_entry(entry)'' with entry a datacatalog entry instance')
        pass
    
    def get_all_tags_table(self, entry = None, delete_tags_not_in_bq_schema = False, make_field_sheet_df = False):
    
        if entry is None:
            entry = self.table_instance_dc

        if entry is not None:
            tags = self.dc_client.list_tags(entry.name)

            update_table_instance_bq = False
            try:
                if self.__table_id != self.table_instance_bq.table_id:
                    update_table_instance_bq = True
            except:
                update_table_instance_bq = True

            if update_table_instance_bq:
                self.get_table_instance_bq(self.__table_id)

            tag_columns = []
            tag_list = []


            if make_field_sheet_df:
                field_vals = [[] for i in range(len(self.__field_keys_to_ndx))]
                field_names = []

            for tag in tags:
                if tag.template == self.field_tag_template.name:

                    tag_column_lower = tag.column.lower()

                    if tag_column_lower in self.schema_bq_df.index:

                        tag_columns.append(tag_column_lower)
                        tag_list.append(tag)


                        if make_field_sheet_df:

                            field_names.append(tag_column_lower)

                            for attr in self.__field_keys_to_ndx.keys():

                                if attr in tag.fields.keys():

                                    field_vals[self.__field_keys_to_ndx[attr]].append(tag.fields[attr].string_value)

                                else:
                                    field_vals[self.__field_keys_to_ndx[attr]].append('')

                    else:
                        if delete_tags_not_in_bq_schema:
                            self.dc_client.delete_tag(tag.name)
                else:
                    tag_columns.append(tag.column)
                    tag_list.append(tag)


            if make_field_sheet_df:
                field_tags_df = pd.DataFrame.from_dict(dict(zip(self.__field_tag_fields_keys_ordered, field_vals))).set_index(pd.Index(field_names)).applymap(lambda x: '' if x is None else x).astype(str).fillna('')

                self.sheet = self.schema_bq_df.join(field_tags_df,lsuffix='_bq', rsuffix='_dc').fillna('')


                n_cols = len(self.sheet.columns)
                
                self.sheet.insert(n_cols - 1,'field_description', [
                    row['field_description_dc']
                    if ( row['field_description_bq'] is None or len(row['field_description_bq']) == 0 )
                    else row['field_description_bq']
                    if ( len(row['field_description_dc']) == 0 or len(row['field_description_dc']) < len(row['field_description_bq']) )
                    else row['field_description_bq']+row['field_description_dc'][self.bq_max_string_length:]
                    if len(row['field_description_bq']) == self.bq_max_string_length
                    else row['field_description_dc']
                    for index, row in self.sheet.iterrows()
                ])

                self.sheet = self.sheet.drop(columns=['field_description_dc', 'field_description_bq']).astype(str).fillna('').set_index('field_name')

            self.tags = dict(zip(tag_columns, tag_list))

            if len(self.tags) == 0:
                self.tags = None

        else:
            raise Exception('\nNo datacatalog entry instance provided. Call method again as ''list_all_tags_entry(entry)'' with entry a datacatalog entry instance')
        pass



    def lookup_and_list_all_tags_entry(self, table_id):
        self.list_all_tags_entry(self.get_table_instance_dc(table_id))
        pass
            
    def delete_all_tags_entry(self, entry = None):
        """
        Deletes all the tags attached to an entry (here an entry is a table instance)
        """
        if entry is None:
            entry = self.table_instance_dc

        if entry is not None:
            for tag in self.dc_client.list_tags(entry.name):
                self.dc_client.delete_tag(tag.name)
        else:
            raise Exception('\nNo datacatalog entry instance provided. Call method again as ''delete_all_tags_entry(entry)'' with entry a datacatalog entry instance')
        pass
    
    def get_bq_schema_metadata(self, table_instance_bq = None):

        if table_instance_bq is None:
            table_instance_bq  = self.table_instance_bq

        if table_instance_bq is not None:

            self.schema_bq = table_instance_bq.schema[:]

            self.schema_bq_df = pd.DataFrame.from_records((schemafield._key()[0:4] for schemafield in self.schema_bq), columns = ['field_name', 'field_type', 'field_mode', 'field_description']).applymap(lambda x: '' if x is None else x).astype('str').assign(field_name_lower=lambda x: x.field_name.apply(lambda y: y.lower())).set_index('field_name_lower').fillna('')

        else:
            raise Exception('\nNo BigQuery table instance provided. Call method again as ''get_bq_schema_metadata(entry)'' with entry a BigQuery table instance')

        pass

    def update_field_tag(self, field_entry_dict, table_instance_dc = None, dict_tags = None):
        """
        This function tries to find a field tag with a column field (which is the actual table field name \
        and in the code below accessed by tag.column) equals the requested field name as specified with \
        field_entry_dict['field_name'].
        If such a tag can be found in the DataCatalog for the table instance, then it checks whether the \ 
        field attributes ( specified as the field values of the tag.fields[key] below, where key is a specific \
        tag field attribute name (field_format, field_description, field_example)) of the DataCatalog tag have \
        different values as the requested/new tag field attributes (which are specified as the values of \ 
        field_entry_dict['field_attributes'][key] where key is again a tag field attribute name)
        Only if the new field attribute values differ from the ones in the tag already on Datacatalog,
        the tag will be updated.
        
        The function returns:
            - True: when the tag has either been updated or does not need to be updated
            - False: when the requested tag has not been found, indictating the tag needs to be newly created
        """

        if table_instance_dc is None:
            table_instance_dc = self.table_instance_dc

        if dict_tags is None:
            dict_tags = self.tags
        
        found_tag = False
        
        if dict_tags is not None:
        
            field_name = field_entry_dict['field_name']
        
            try:
                tag = dict_tags[field_name]
                found_tag = True
            except KeyError:
                pass
    
            update_tag = False
            
            if found_tag:
                for key, value in field_entry_dict['field_attributes'].items():
                    if len(value) > 0:
                        if key in self.field_tag_template.fields.keys():
                            if tag.fields[key].string_value != value:
                                tag.fields[key].string_value = value
                                update_tag = True
            if update_tag:
                self.dc_client.update_tag(tag)
            
        return found_tag
    
    def create_field_tag(self, field_entry_dict, table_instance_dc = None, dict_tags = None):
        """
        This function creates a field tag for a table instance (which is not the table name! \
        An instance object is return by the datacatalog.lookup_entry function and the name member of that
        instance is used as the parent when creating the tag with datacatalog.create_tag.
        
        Input:
            - table_instance_dc: an instance of a table (we get the instance via the lookup_entry method\
              of the datacatalog_v1beta1.DataCatalogClient class)
            - field_entry_dict: a dictionary containg the field attributes and corresponding values of the sadc_fieldstored as a dic
        """

        if table_instance_dc is None:
            table_instance_dc = self.table_instance_dc

        if dict_tags is None:
            dict_tags = self.tags
        
        if(not self.update_field_tag(field_entry_dict, table_instance_dc = table_instance_dc, dict_tags = dict_tags)):
            
            new_field_tag = datacatalog_v1beta1.types.Tag()
    
            new_field_tag.template = self.field_tag_template.name
    
            create_tag = False
        
            field_name = field_entry_dict['field_name']
        
            if(field_name != ''):
                for key, value in field_entry_dict['field_attributes'].items():
                    if len(value) > 0:
                        if key in self.field_tag_template.fields.keys():
                            new_field_tag.fields[key].string_value = value
                            create_tag = True
                if(create_tag):
                    new_field_tag.column = field_name
            if create_tag:
                try:
                    self.dc_client.create_tag(parent=table_instance_dc.name,tag=new_field_tag)
                except:
                    self.to_log('\t\tProblem to write tag to field {} of table {}\n'.format(field_name, self.__table_id))
                        
        pass

    def update_table_tag(self, table_entry_dict, table_instance_dc = None, dict_tags = None):
        """
        This function tries to find a table tag for the table instance.
        If such a tag can be found in the DataCatalog, then it checks whether the field attributes \
        ( specified as the field values of the tag.fields[key] below, where key is a specific \
        tag field attribute name (table_description, table_data_source)) of the DataCatalog tag have \
        different values as the requested/new field attributes (which are specified as the values of \ 
        table_entry_dict[key] where key is again a tag field attribute)
        Only if the new tag field attribute values differ from the ones in the tag already on Datacatalog,
        the tag will be updated.
        
        The function returns:
            - True: when the tag has either been updated or does not need to be updated
            - False: when the requested tag has not been found, indictating the tag needs to be newly created
        """

        if table_instance_dc is None:
            table_instance_dc = self.table_instance_dc

        if dict_tags is None:
            dict_tags = self.tags
        
        found_tag = False
        
        if dict_tags is not None:
        
            try:
                tag = dict_tags['']
                if tag.template == self.table_tag_template.name:
                    found_tag = True
            except KeyError:
                pass
    
            update_tag = False
            
            if found_tag:
                for key, value in table_entry_dict.items():
                    if len(value) > 0:
                        if key in self.table_tag_template.fields.keys():
                            if tag.fields[key].string_value != value:
                                tag.fields[key].string_value = value
                                update_tag = True
            if update_tag:
                self.dc_client.update_tag(tag)
            
        return found_tag
    
    def create_table_tag(self, table_entry_dict, table_instance_dc = None, dict_tags = None):

        if table_instance_dc is None:
            table_instance_dc = self.table_instance_dc

        if dict_tags is None:
            dict_tags = self.tags

        
        if(not self.update_table_tag(table_entry_dict, table_instance_dc, dict_tags)):
        
            new_table_tag = datacatalog_v1beta1.types.Tag()
    
            new_table_tag.template = self.table_tag_template.name
    
            create_tag = False
    
            for key, value in table_entry_dict.items():
                if len(value) > 0:
                    if key in self.table_tag_template.fields.keys():
                        new_table_tag.fields[key].string_value = value
                        create_tag = True
            if create_tag:
                self.dc_client.create_tag(parent=table_instance_dc.name,tag=new_table_tag)
        pass

    def download(self, tables = None, DS_ID = None, PATH=None):
        """
        Downloads metadata of tables in a dataset specified by DS_ID.
        - By default metadata for all tables in the dataset is downloaded in an Excel .xlsx file in a folder that has the name of the dataset. For each table a separate sheet of that .xlsx file is created containing the field_names, field_descriptions and more.
        - Specifying the parameter 'tables' allows to download metadata for a single or a list of tables.
        - For all tables in the dataset table tags metadata is written to a sheet with the name 'metadata_of_tables'
        - The PATH specifies the path where the metadata shall be written.

        Parameters
        ----------
        tables: String, List of Strings, None (default)
            A String or List of Strings specifying the table_ids for which metadata should be downloaded.
            If not provided, metadata for all tables in the dataset is downloaded

        DS_ID: String, None (default)
            dataset_id for which metadata shall be downloaded.
            If no dataset_id is provided via DS_ID, the one specified by the member attribute .DS_ID is used which is by default 'sadc_generated'.

        PATH: String, None (default)
            The PATH where the metadata shall be written.
        """
        assert isinstance(tables, list) or isinstance(tables, str) or tables is None, "'Tables' parameter must be String, List or None"
        assert isinstance(DS_ID, str) or DS_ID is None, "'DS_ID' parameter must be String or None"
        assert isinstance(PATH, str) or PATH is None, "'PATH' parameter must be String or None"

        DS_ID_old = self.DS_ID
        if DS_ID is None:
            DS_ID = self.DS_ID
        else:
            self.set_dataset(DS_ID)

        if DS_ID is not None:

            if not self.__update:

                self.to_log('# Download\n')

                if PATH is not None:
                    PATH_OUT = os.path.join(PATH, DS_ID)
                else:
                    PATH_OUT = os.path.join(self.CDIR, DS_ID)

                if not os.path.exists(PATH_OUT):
                    os.makedirs(PATH_OUT)
            else:
                self.to_log('\n\t# Download\n')

            self.overview_sheet = construct_overview_sheet(self.table_tag_template, attributes = self.__table_tag_fields_keys_ordered)

            table_sheets = construct_table_sheets()
            

            if tables is None:

                tables = self.bq_client.list_tables("{}.{}".format(self.P_ID, self.DS_ID))

            elif isinstance(tables, str):

                tables = [tables]
                
            for table in tables:
                try:
                    self.__table_id = table.table_id
                except:
                    self.__table_id = table

                self.to_log('\t{}'.format("Table '{}'".format(self.__table_id)))

                self.to_log('\t\t{}'.format('get BigQuery table instance'))
                self.get_table_instance_bq(self.__table_id)
                self.to_log('\t\t{}'.format('get DataCatalog table instance'))
                self.get_table_instance_dc(self.__table_id)
                self.to_log('\t\t{}'.format('get all tags and create dataframe with out of field tags and BigQuery schema'))
                self.get_all_tags_table(make_field_sheet_df = True)

                self.to_log('\t\t{}'.format('append fields dataframe to dict'))
                table_sheets.append(self.__table_id, self.sheet)

                self.to_log('\t\t{}'.format('append table tag to overview sheet variable'))
                self.append_to_overview_sheet()
            
            self.to_log('\n\t{}'.format('make Dictionary out of field metadata dataframes for all specified tables'))
            self.ds_field_tags_dicts = table_sheets.get_dict()
            self.to_log('\t{}'.format('make Dataframe out of table tag metadata for all specified tables'))
            self.ds_table_tags_df = self.overview_sheet.get_dataframe()


            if not self.__update:

                FULLPATH = os.path.join(PATH_OUT, DS_ID+'.xlsx')

                self.to_log('\twrite to {}\n'.format(FULLPATH))


                with pd.ExcelWriter(FULLPATH) as writer:
                    self.ds_table_tags_df.to_excel(writer, sheet_name=self.overview_sheet_name, header=True, index=True)
                    for table_id, table_df in self.ds_field_tags_dicts.items():
                        table_df.to_excel(writer, sheet_name=self.shorten_string(table_id, self.excel_max_sheet_name_length), header=True, index=True)


            self.set_dataset(DS_ID_old)

        else:
            raise Exception("No Dataset specified. Please call the function as 'download(DS_ID=dataset_id)' again with dataset_id a string specifying a dataset ID")
        pass


    def append_to_overview_sheet(self):
        table_description_bq = self.table_instance_bq.description
        table_description_bq = self.clean_sentence_string(self.pure_table_description_bq(table_description_bq))

        dict_table_descr_bq = None
        if len(table_description_bq) > 0:
            dict_table_descr_bq = {'table_description': table_description_bq}
    
        try:
            table_tag = self.tags['']

            if self.__prefer_bq_for_downlad_update:
                if len(table_description_bq) > 0:
                    self.overview_sheet.append(self.__table_id, table_tag, dict_table_descr_bq)
                else:
                    self.overview_sheet.append(self.__table_id, table_tag)
            else:
                self.overview_sheet.append(self.__table_id, table_tag)
        except:
            self.overview_sheet.append(self.__table_id, alt_tag_vals = dict_table_descr_bq)
        pass




    def upload(self, tables = None, DS_ID = None, PATH = None, delete_old_tags_before_upload = False, delete_sheet_after_upload = True, upload_from_backup = False):
        """
        uploads metadata of tables in a dataset specified by DS_ID.
        - By default metadata for all tables in the dataset is uploaded from an Excel .xlsx file in a folder that has the name of the dataset. For each table a separate sheet of that .xlsx file is created containing the field_names, field_descriptions and more.
        - Specifying the parameter 'tables' allows to download metadata for a single or a list of tables.
        - For all tables in the dataset table tags metadata is in a sheet with the name 'metadata_of_tables'
        - The PATH specifies the path where the Excel .xlsx file is contained.

        Parameters
        ----------
        tables: String, List of Strings, None (default)
            A String or List of Strings specifying the table_ids for which metadata should be downloaded.
            If not provided, metadata for all tables in the dataset is downloaded

        DS_ID: String, None (default)
            dataset_id for which metadata shall be downloaded.
            If no dataset_id is provided via DS_ID, the one specified by the member attribute .DS_ID is used which is by default 'sadc_generated'.
        
        PATH: String, None (default)
            The PATH where the metadata shall be read from.

        delete_old_tags_before_upload: True, False (Default)
            If set to True it deletes all tags in the datacatalog for a table instance before writing new ones. If set False the tags in datacalog are updated with the new information but not deleted.

        delete_sheet_after_upload: False, True (Default)
            If True, the folder including the sheet that has been uploaded will be deleted.

        upload_from_backup: True, False (Default)
            if True, use the backup Excel sheets for upload
        """
        assert isinstance(tables, list) or isinstance(tables, str) or tables is None, "'Tables' parameter must be String, List or None"
        assert isinstance(DS_ID, str) or DS_ID is None, "'DS_ID' parameter must be String or None"
        assert isinstance(PATH, str) or PATH is None, "'PATH' parameter must be String or None"

        DS_ID_old = self.DS_ID
        if DS_ID is None:
            DS_ID = self.DS_ID
        else:
            self.set_dataset(DS_ID)

        self.delete_old_tags_before_upload = delete_old_tags_before_upload

        if DS_ID is not None:


            if not self.__update:

                self.to_log('\n# Upload\n')

                if PATH is None:
                    PATH = os.path.join(self.CDIR, DS_ID)
                if upload_from_backup:
                    PATH = os.path.join(os.path.join(self.CDIR, 'backup_sheets'), DS_ID)

                excel_files = glob.glob(os.path.join(PATH, r"*.xlsx"))
                assert len(excel_files) > 0, "No .xlsx files under the path {}".format(PATH)

                FULLPATH = os.path.join(PATH, DS_ID+'.xlsx')

                try:
                    self.ds_table_tags_df = pd.read_excel(FULLPATH, sheet_name=self.overview_sheet_name, index_col  = 0, dtype  = str).fillna('').astype(str).applymap(lambda x: x.strip())
                except:
                    msg = 'Reading {} was not successful. Check path and existence of file.'.format(FULLPATH)
                    self.to_log('\t\n{}\n'.format(msg))
                    raise Exception(msg)

                if tables is None:
                    tables = self.ds_table_tags_df.index.to_list()
                else:
                    diff_keys_set = set(tables) - set(self.ds_table_tags_df.index)
                    assert len(diff_keys_set) == 0, "The tables {} are not contained in the spreadsheet.".format(diff_keys_set)

                table_to_ndx = {table_id: k+1 for k, table_id in enumerate(self.ds_table_tags_df.index) if table_id in tables}

                self.ds_field_tags_dicts = pd.read_excel(FULLPATH, sheet_name=list(table_to_ndx.values()), index_col = 0, dtype  = str)

            else:

                if tables is None:
                    tables = self.ds_table_tags_df.index.to_list()
                else:
                    diff_keys_set = set(tables) - set(self.ds_table_tags_df.index)
                    assert len(diff_keys_set) == 0, "The tables {} are not contained in the spreadsheet.".format(diff_keys_set)

                self.to_log('\n\t# Upload\n')
                table_to_ndx = {table_id: table_id for table_id in self.ds_table_tags_df.index if table_id in tables}

            for table_id, k in table_to_ndx.items():

                self.to_log('\t{}'.format("Table '{}'".format(table_id)))
                self.__table_id = table_id
                self.to_log('\t\t{}'.format('get BigQuery table instance'))
                self.get_table_instance_bq(table_id)
                self.to_log('\t\t{}'.format('get DataCatalog table instance'))
                self.get_table_instance_dc(table_id)

                self.to_log('\t\t{}'.format('get all tags'))
                self.get_all_tags_table(delete_tags_not_in_bq_schema=True)

                self.to_log('\t\t{}'.format('create table tag dictionary'))
                self.table_tag_dict = dict(self.ds_table_tags_df.loc[table_id])

                self.to_log('\t\t{}'.format('upload table tag'))
                self.upload_table_tag()

                self.to_log('\t\t{}'.format('upload BigQuery table description'))
                self.__upload_table_description_bq()

                self.sheet = self.ds_field_tags_dicts[k].fillna('').astype(str).applymap(lambda x: x.strip())

                self.to_log('\t\t{}'.format('upload BigQuery and DataCatalog field information'))
                self.upload_fields_sheet()

            if not self.__update and delete_sheet_after_upload and not upload_from_backup:
                shutil.rmtree(PATH)

            self.set_dataset(DS_ID_old)
            self.write_log()

        else:
            raise Exception("No Dataset specified. Please call the function as 'upload(DS_ID=dataset_id)' again with dataset_id a string specifying a dataset ID")
        pass



    def synchronize(self, tables = None, DS_ID = None):
        """
        Synchronizes metadata between Bigquery and Datacatalog of tables in a dataset specified by DS_ID.
        - By default metadata for all tables in the dataset is downloaded in an Excel .xlsx file in a folder that has the name of the dataset. For each table a separate sheet of that .xlsx file is created containing the field_names, field_descriptions and more.
        - Specifying the parameter 'tables' allows to download metadata for a single or a list of tables.
        - For all tables in the dataset table tags metadata is written to a sheet with the name 'metadata_of_tables'

        Parameters
        ----------
        tables: String, List of Strings, None (default)
            A String or List of Strings specifying the table_ids for which metadata should be downloaded.
            If not provided, metadata for all tables in the dataset is downloaded

        DS_ID: String, None (default)
            dataset_id for which metadata shall be downloaded.
            If no dataset_id is provided via DS_ID, the one specified by the member attribute .DS_ID is used which is by default 'sadc_generated'.

        """
        assert isinstance(tables, list) or isinstance(tables, str) or tables is None, "'Tables' parameter must be String, List or None"
        assert isinstance(DS_ID, str) or DS_ID is None, "'DS_ID' parameter must be String or None"

        DS_ID_old = self.DS_ID
        if DS_ID is None:
            DS_ID = self.DS_ID
        else:
            self.set_dataset(DS_ID)

        if DS_ID is not None:

            self.to_log('\n# Synchronize\n')

            self.__update = True

            self.download(tables=tables, DS_ID = DS_ID)
            self.upload(tables=tables, DS_ID = DS_ID, delete_sheet_after_upload = False)

            self.__update = False
        else:
            raise Exception("No Dataset specified. Please call the function as 'synchronize(DS_ID=dataset_id)' again with dataset_id a string specifying a dataset ID")
        pass


    def upload_fields_sheet(self):
        for column_name, row in self.sheet.iterrows():
            if len(column_name) > 0:
                try:
                    # this tries to get a numeric key value for the column name by checking first whether\
                    # the column name is in the table schema of BQ
                    # if it is not found means that this column field is no longer part of the schema
                    # and skips over that entry
                    num_index = self.schema_bq_df.index.get_loc(column_name.lower())

                    has_descr = False
                    if 'field_description' in row.keys():
                        has_descr = True
                        field_description = self.clean_sentence_string(row['field_description'])
                        field_attributes_dc = {**{key: self.clean_string(row[key]) for key in row.keys() if key not in ['field_description']}, 'field_description': self.clean_sentence_string(row['field_description'])}
                    else:
                        field_attributes_dc = {key: self.clean_string(row[key]) for key in row.keys()}
    
                    field_entry_dict = {'field_name': column_name.lower(), 'field_attributes': field_attributes_dc}

                    self.create_field_tag(field_entry_dict)
                    
                    field_bq = self.schema_bq[num_index]
                    field_bq_name = field_bq.name
                    field_bq_field_type = field_bq.field_type
                    field_bq_mode = field_bq.mode
    
                    if has_descr:
                        field_description_bq = self.shorten_string(field_description, self.bq_max_string_length)
                        self.schema_bq[num_index] = bigquery.SchemaField(name=field_bq_name, field_type=field_bq_field_type, mode=field_bq_mode, description=field_description_bq)

                except KeyError:
                    pass
            else:
                break
    
        self.check_non_matching_columns_bq_excel()
    
        self.table_instance_bq.schema = self.schema_bq
        num_trials = 1
        update_schema = False
        while num_trials < 11 and not update_schema:
            try:
                self.table_instance_bq = self.bq_client.update_table(self.table_instance_bq, ["schema"])
                update_schema = True
            except Exception as e:
                if hasattr(e, 'message'):
                    err = e.message
                else:
                    err = e
                num_trials = num_trials + 1
                if num_trials == 11:
                    self.to_log("\t\t\terror while trying to write schema to BigQuery:")
                    self.to_log(err)
                    self.to_log("\t\t\terror occured, this was the last attempt\n")
                else:
                    self.to_log("\t\t\terror while trying to write schema to BigQuery:\n")
                    self.to_log(err)
                    self.to_log("\t\t\terror occured, start {}. attempt\n".format(num_trials))
                pass

    def upload_table_tag(self):
        diff_keys_set = set(self.table_tag_dict.keys()) - set(self.table_tag_template.fields.keys())
        assert len(diff_keys_set) == 0, "The attribute names {} are no attribute names of the tag template {}".format(diff_keys_set, self.table_tag_template.name)
        self.create_table_tag(self.table_tag_dict)


    def __upload_table_description_bq_init(self):
        """
        This function is only executed during initialisation of the class instance to set parameter for the function upload_table_description_bq
        """

        self.__table_attrs = [attr for attr in self.table_tag_template.fields.keys() if attr not in ['table_description']]

        max_str_len_extra_metadata_keys = reduce((lambda x,y: max(x,y)), map( lambda x: len(x) , self.table_tag_template.fields.keys()) )

        self.__n_int_tab = 5
        self.__max_n_tabs = (max_str_len_extra_metadata_keys+1)//self.__n_int_tab


    def __upload_table_description_bq(self):


        table_description = self.clean_sentence_string(self.table_tag_dict['table_description'])

        extra_metadata_string = '\n\nTable attributes:\n\n'
        
        has_extra_metadata = False

        for column in self.__table_attrs:
            if len(self.table_tag_dict[column]) > 0:
                has_extra_metadata = True
                column_first_part = column[6:9]
                if column[6:9] == 'gcp':
                    column_first_part = 'GCP'
                else:
                    column_first_part = column[6].upper() + column[7:9]
                n_tabs = self.__max_n_tabs - ((len(column)+1)//self.__n_int_tab) + 1
                extra_metadata_string = extra_metadata_string + column_first_part \
                                        + re.sub(r'_+',' ', column[9:]) + ":" + "\t"*n_tabs \
                                        + self.table_tag_dict[column]
            if extra_metadata_string[-1] != '\n':
                extra_metadata_string = extra_metadata_string + "\n"
        
        if has_extra_metadata:
            self.table_instance_bq.description = table_description + extra_metadata_string
        else:
            self.table_instance_bq.description = table_description
        self.table_instance_bq = self.bq_client.update_table(self.table_instance_bq, ["description"])

        pass


    def check_non_matching_columns_bq_excel(self, table_instance_dc = None, excel_column_names = None, bq_column_names = None):

        if table_instance_dc is None:
            table_instance_dc = self.table_instance_dc

        if excel_column_names is None:
            excel_column_names = self.sheet.index

        if bq_column_names is None:
            bq_column_names = self.schema_bq_df.index

        set_excel_column_fields = set(excel_column_names.map(lambda x: x.lower()))
        set_bq_column_fields = set(bq_column_names)
    
        set_not_in_bq = set_excel_column_fields.difference(set_bq_column_fields)
        set_not_in_excel = set_bq_column_fields.difference(set_excel_column_fields)
    
        if bool(set_not_in_bq) or bool(set_not_in_excel):
            self.to_log('\t\t\tFor the table at the BigQuery path\n \'{}\''.format(table_instance_dc.linked_resource))
            self.to_log('\t\t\tIn the following list, entries prefixed with:')
            self.to_log('\t\t\t \'<\':\tare contained in the Excel spreadsheet but not in the BigQuery table schema (anymore).\n\t\t\tPlease delete them in the Excel spreadsheet!')
            self.to_log('\t\t\t \'>\':\tare contained in the BigQuery table schema but not in the Excel spreadsheet.\n\t\t\t\tPlease add them in the Excel spreadsheet!\n')
            
            if bool(set_not_in_bq):
                for column_name in set_not_in_bq:
                    self.to_log('\t\t\t\t< {}'.format(column_name))
    
            if bool(set_not_in_excel):
                if bool(set_not_in_bq):
                    self.to_log('\n')
                    
                for column_name in set_not_in_excel:
                    self.to_log('\t\t\t\t> {}'.format(column_name))


    def to_log(self, message = None):
        if isinstance(message, str):
            if self.__do_log:
                    self.__log = self.__log + message
            if self.__do_print_log:
                print(message)
        pass

    def write_log(self):
        if self.__do_log:
            F = open(self.__logfile, "w") 
            F.write(self.__log)
            self.__log = ''
            F.close() 

    @staticmethod
    def clean_string(string):
        string = string.strip()
        if len(string) > 0:
            string = re.sub(r'\s+',' ', string)
        return string
    
    @classmethod
    def clean_sentence_string(cls, string):
        string = cls.clean_string(string)
        if len(string) > 0:
            string = string[0].upper() + string[1:]
            if string[-1] !=  r"." and string[-1] != r"]":
                string = string + r"."
        return string


    @staticmethod
    def shorten_string(string, n):
        if len(string) < n:
            return string
        else:
            return string[:n]

    @classmethod
    def pure_table_description_bq(cls, table_description_bq):
        if table_description_bq is not None:
            try:
                table_description_bq_pure = cls.pattern_table_descr_bq_pure.search(table_description_bq).group('descr')
            except:
                table_description_bq_pure = table_description_bq
            return table_description_bq_pure
        else:
            return ''


class construct_overview_sheet:
    def __init__(self, tag_template, attributes = None):
        self.__dict_attributes = {item[0]: k for k, item in enumerate(tag_template.fields.items())}
        self.__num_el = len(self.__dict_attributes)
        self.__list_attributes = [[] for i in range(self.__num_el)]
        self.__list_table_id = []

        if attributes is None:
            self.__attributes_ordered = list(tag_template.fields.keys())
        else:
            assert isinstance(attributes, list), "'attributes' parameter must be a list"
            assert len(set(tag_template.fields.keys()) - set(attributes)) == 0, "The provided attributes are no permutation of the field keys of the provided tag_template"
            self.__attributes_ordered = attributes

    def append(self, table_id, tag = None, alt_tag_vals = None):
        assert isinstance(alt_tag_vals, dict) or alt_tag_vals is None, "'alt_tag_vals' must be of type dict or None"
        if alt_tag_vals is None:
            alt_tag_vals = {}

        self.__list_table_id.append(table_id)
        if tag is not None:
            for attr, index in self.__dict_attributes.items():
                alt_val_not_avail = True
                if attr in alt_tag_vals.keys():
                    self.__list_attributes[index].append(alt_tag_vals[attr])
                    alt_val_not_avail = False
                        
                if alt_val_not_avail:
                    try:
                        if(attr == 'table_description'):
                            self.__list_attributes[index].append(toolbox.clean_sentence_string(tag.fields[attr].string_value))
                        else:
                            self.__list_attributes[index].append(tag.fields[attr].string_value)
                    except:
                        self.__list_attributes[index].append('')
        else:
            for attr, index in self.__dict_attributes.items():
                if attr in alt_tag_vals.keys():
                    self.__list_attributes[index].append(alt_tag_vals[attr])
                else:
                    self.__list_attributes[index].append('')

    def get_dataframe(self):
        return pd.DataFrame.from_dict({'table_id': self.__list_table_id, **{attr: self.__list_attributes[index] for attr, index in self.__dict_attributes.items()}}).fillna('').astype(str).applymap(lambda x: x.strip()).set_index('table_id')[self.__attributes_ordered]

    def set_datframe(self, return_df = False):
        self.df = self.get_dataframe()
        if return_df:
            return self.df
        else:
            pass

class construct_table_sheets:
    def __init__(self):
        self.__list_table_id = []
        self.__list_of_sheet_df = []

    def append(self, table_id, sheet):

        self.__list_table_id.append(table_id)

        self.__list_of_sheet_df.append(sheet)

    def get_dict(self):
        return OrderedDict(zip(self.__list_table_id, self.__list_of_sheet_df))

    def set_dict(self, return_dict = False):
        self.dict_sheets = self.get_dict()
        if return_dict:
            return self.dict_sheets
        else:
            pass
