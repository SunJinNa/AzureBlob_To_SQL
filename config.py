"""
Sun J, 2020/03/23:
    This config.py file reads configuration details input in credentials.config. 
    It will create global objects.
"""

# modules needed for reading local configurations
import configparser
import sys
import sqlalchemy as sa
from pathlib import Path
from azure.storage.blob import BlockBlobService

from helper import write_log


# read information except for blob credentials
try:
    # read configuration file
    project_folder = Path.cwd() #current working directory
    configPath = project_folder / 'credentials.config'
    config = configparser.ConfigParser() #a configparser object -> read config.py into a class
    config.read_file(open(configPath, 'r'))
    append_option = config['Append Option']['append_option']

    # read credentials 
    server_name = config['SQL']['server']
    db_name = config['SQL']['database_name']
    db_username = config['SQL']['db_username']
    db_password = config['SQL']['db_password']

    # define connection strings: this will not attempt to connect thus no error will occur
    trusted_engine = sa.create_engine(f'mssql+pyodbc://{server_name}/{db_name}?' \
                                    'driver=SQL+Server+Native+Client+11.0&' \
                                    'Trusted_Connection=yes')
    db_engine = sa.create_engine(f'mssql+pyodbc://{db_username}:{db_password}@{server_name}/{db_name}?' \
                                'driver=SQL+Server+Native+Client+11.0&' \
                                'Trusted_Connection=no')
except IOError: write_log("The config file is not present in the project folder."); sys.exit()
else: write_log("A new process has started running." 
        + "All information in credentials.config file have been read.") #only run if no error
    

# detect whether user wants to download from blob or simply upload files from local 
try:
    blob_acc_name = config['Azure']['blob_acc_name']
    blob_acc_key = config['Azure']['blob_acc_key']
    blob_container_name = config['Azure']['blob_container_name']
    blob_folder_name = config['Azure']['blob_folder_name']
    blob_archive_name = config['Azure']['blob_archive_name']
    blob_access = BlockBlobService(account_name=blob_acc_name, account_key=blob_acc_key) #throws an error
    blob_flag = 1
except ValueError:
    blob_access = ""
    blob_flag = 0
    write_log("No Azure crendentials are detected, no file from Azure will be downloaded."
        + "Moving on to process existing data in the downloaded folder.")




                              
                              