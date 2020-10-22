"""
Sun J, 2020/03/23:
    This helper.py defines all the functions and classes needed by the tool.
"""


# all the modules needed globally in this script
import re
import subprocess
import numpy as np
import pandas as pd
import sqlalchemy as sa
import sys
import csv

from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from tqdm import tqdm
from sqlalchemy import Column, Integer, String, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import InterfaceError
from sqlalchemy.orm import sessionmaker


# write log message to record running of the process
def write_log(msg, log_file = "run_log.txt"):
    """
    This function attempts to write message to a log file in the same directory and prints the message.
    It will stop if user has no writing permission.
        Returns None.  
        @msg: a string representing the log message
        @log_file: logfile's path
    """
    print("\n" + msg + "\n")
    try:
        row = []
        row.append(datetime.now().strftime('%Y-%m-%d %H:%M'))
        row.append(msg)
        row = ", ".join(row)
        with open(log_file, "a", newline = '\n') as l:
            l.write(row + "\n")
    except Exception as e: 
        print(f"Failed to write to the run_log.txt file due to error '{e}'.")
        sys.exit()


class connection_check:
    """
    This class tests connections to azure blob and sql server.
        @blob_container_name: blob container name used to test connection
        @trusted_engine: sql engine created with widnows credentials
        @db_engine: sql engine created with sql account credentials
        @blob_flag: boolean with 0 indicating no data needs to be downloaded from the blob
    """

    def __init__(self, **kwargs):
        # blob arguments
        if 'blob_container_name' in kwargs: self._blob_container_name = kwargs['blob_container_name']
        if 'blob_access' in kwargs: self._blob_access = kwargs['blob_access']
        if 'blob_folder_name' in kwargs: self._blob_folder_name = kwargs['blob_folder_name']
        if 'blob_archive_name' in kwargs: self._blob_archive_name = kwargs['blob_archive_name']
        if 'blob_flag' in kwargs: self._blob_flag = kwargs['blob_flag']

        # sql arguments 
        if 'trusted_engine' in kwargs: self._trusted_engine = kwargs['trusted_engine']
        if 'db_engine' in kwargs: self._db_engine = kwargs['db_engine']
        if 'server_name' in kwargs: self._server_name = kwargs['server_name']
        if 'db_name' in kwargs: self._db_name = kwargs['db_name']
        if 'db_username' in kwargs: self._db_username = kwargs['db_username']
        if 'db_password' in kwargs: self._db_password = kwargs['db_password']
        
        # other arguments
        if 'append_option' in kwargs: self._append_option = kwargs['append_option']
        if 'downloaded_folder' in kwargs: self._downloaded_folder = kwargs['downloaded_folder']
        if 'processed_folder' in kwargs: self._processed_folder = kwargs['processed_folder']

    def test_connection_blob(self):
        """This function tests connection to azure blob. Returns None"""
        try:
            write_log("Attempting to connect to Azure Blob.")
            self._blob_access.list_blobs(self._blob_container_name)
            write_log("Successfully connected to Azuer Blob.")
        except Exception as e:
            write_log(f"Failed to connect to Azure Blob due to error '{e}'.")
            sys.exit()

    def test_connection_db(self):
        """This function tests connection to sql server. Returns None."""
        write_log("Attemping to connect to SQL server.")
        global trusted_connection_flag
        try:
            conn = self._trusted_engine.connect()
            conn.close()
            trusted_connection_flag = 1
            write_log("Successfully connected to SQL server with windows credentials.")
        except InterfaceError: 
            conn = self._db_engine.connect()
            conn.close()
            trusted_connection_flag = 0
            write_log("Successfully connected to SQL server with SQL account credentials.")
        except:
            write_log(f"Failed to connect to SQL server due to error '{e}'.")
            sys.exit()

    def connection_check_master(self):
        """This function governs connection check process in this class. Returns None"""
        if self._blob_flag == 1: self.test_connection_blob()
        self.test_connection_db()


class data_cleansing(connection_check):
    """
    This class downloads data from blob (if applicable), extract zipped files (if applicable) 
    and convert Excel files into csv format (if applicable). Compatible zipped file formats
    include .zip, .tar, .rar, .7z. Parameters are inherited from connection_check class. 
        **@inheritance
        @downloaded_folder: local folder to which files on azure should be downloaded.
        @processed_folder: local folder to which processed files should be moved.
        @blob_folder_name: blob folder where the data is stored
    """


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    

    def move_to_processed(self, file):
        """
        This function moves the file from downloaded folder to processed folder.
            Returns None.
            @file: file full path
        """                
        source = self._downloaded_folder / f'{file.name}'
        destination = self._processed_folder / f'{file.name}'
        source.replace(destination) 


    def detect_file_separator(self, file):
        """
        This function attempts to detect csv files' separators ('^',',',';','|').
            Returns a string representing csv's separator.
            @file: a string specifying the csv's file path.
        """
        with open(file) as f:
            while True: 
                # current line
                line = f.readline()
                dialect = csv.Sniffer().sniff(line,delimiters=['^',',',';','|'])
                delimiter = dialect.delimiter

                # next line 
                next_line = f.readline()
                next_dialect = csv.Sniffer().sniff(line,delimiters=['^',',',';','|'])
                next_delimiter = dialect.delimiter

                # stop if detect the same delimiters from line to line
                if delimiter == next_delimiter: return delimiter


    def download_data_blob(self):
        """
        This function downloads data from azure blob to the local downloaded folder. It attempts 
        to move downloaded files into the given archive folder in azure blob as well. 
            Returns None.
        """
        try:
            write_log(f"Attempting to download data from Azure Blob to {self._downloaded_folder}")
            self._downloaded_folder.mkdir(exist_ok=True)
            blobs = self._blob_access.list_blobs(self._blob_container_name, prefix=self._blob_folder_name)
            for file in blobs:
                if '.signiant' not in file.name: #.signiant is present on azure by default in every folder
                    # download files in the given folder 
                    write_log(f'--Attempting to download {file.name} from Azure Blob.')
                    self._blob_access.get_blob_to_path(self._blob_container_name, file.name, self._downloaded_folder / file.name.split('/')[-1])
                    
                    # Move downloaded files to Archive folder
                    if self._blob_archive_name != "":
                        new_blob_url = self._blob_access.make_blob_url(self._blob_container_name, file.name)
                        archive_blob_path = file.name.replace('New', 'Archive')
                        self._blob_access.copy_blob(self._blob_container_name, archive_blob_path, new_blob_url)
                        self._blob_access.delete_blob(self._blob_container_name, file.name)
                    write_log(f'--Successfully downloaded {file.name} from Azure Blob.')
            write_log(f'Successfully downloaded all files from Azure Blob to {self._downloaded_folder}')
        except Exception as e: 
            write_log(f"Failed to download all data from Azure Blob to {self._downloaded_folder}"
                    + "due to error '{e}'")
            sys.exit()


    def extract_compressed_files(self):
        """
        This function attempts to extract zipped files, if any, to the downloaded folder. It will 
        then move the original zipped files to the processes folder after they are unzipped.
            Returns None.
        """
        # read all zipped files
        compatible_formats = ('**/*.zip', '**/*.tar', '**/*.rar', '**/*.7z')
        compressed_files = []
        for ext in compatible_formats: compressed_files.extend(self._downloaded_folder.glob(ext)) 

        # stop if there is none
        if len(compressed_files) == 0: 
            write_log("No compressed files are detected. Moving on to process Excel and flat files.")
            return None 
        try:
            write_log("Attempting to unzip all compressed files.")
            from zipfile import ZipFile
            from tarfile import TarFile
            from rarfile import RarFile
            from py7zr import unpack_7zarchive

            # 7z files 
            class py7zrWrapper():
                """A class is created to make it conform to the API of ZipFile/TarFile standard packages. Use context managers and 
                wrapper class around py7zr."""
                def __init__(self, file): self.file = str(file)
                def extractall(self, path_extract_to): unpack_7zarchive(self.file, path_extract_to)
            @contextmanager
            def SevenZFile(file, mode):
                file_7z = py7zrWrapper(file)
                yield file_7z

            # unzip into downloaded folder
            extractor_dict = {'.zip': ZipFile, '.tar': TarFile, '.rar': RarFile, '.7z': SevenZFile}
            for file in tqdm(compressed_files):
                write_log(f"--Attempting to unzip {file.name}.")
                with extractor_dict[file.suffix](file, 'r') as ext:
                    ext.extractall(self._downloaded_folder)
                self.move_to_processed(file)
                write_log(f"--Successfully unzipped {file.name}.")
            write_log("Successfully unzipped all compressed files.")
        except Exception as e:
            write_log(f"Failed to extract all zipped files due to error '{e}'")
            sys.exit()


    def produce_flat_file(self):
        """
        This function attempts to prepare flat files before the final upload step, incl. Excel conversion and appending data.
            Returns None.
        """
        # read all excel files
        files = []
        for ext in ('**/*.xls', '**/*.xlsx'): files.extend(self._downloaded_folder.glob(ext)) 

        # if user does not want to append data, convert excel sheets into csv formats 
        if int(self._append_option) == 0:
            write_log("User chooses not to append any data. All files will be processed separately.")

            #stop if no excel files are present
            if len(files) == 0: 
                write_log("No Excel file is detected. Moving on to upload all flat files to SQL separately.")
                return None 

            # convert excel into csv 
            try:
                write_log("Attempting to convert all Excel files into csv.")
                for file in tqdm(files):
                    write_log(f"--Attempting to convert all visible sheets in {file.name} into csv.")

                    #check hidden sheets
                    sheets = pd.ExcelFile(file).book.sheets()
                    visible_sheets = [i for i in sheets if i.visibility==0]

                    #read visible sheets only
                    for sheet in visible_sheets: 
                        worksheet_df = pd.read_excel(file, sheet_name=sheet.name) 
                        worksheet_name = str(file.stem) + '_' + str(sheet.name) 
                        worksheet_name = worksheet_name.replace(' ', '_')
                        worksheet_df['FileName'] = worksheet_name 

                        #write to csv
                        csv_path = self._downloaded_folder / f'{worksheet_name}.csv'
                        if Path.exists(csv_path): Path.unlink(csv_path) #overwrite existing file
                        worksheet_df.to_csv(csv_path, sep="^")
                    self.move_to_processed(file)
                    write_log(f"--Successfully converted all visible sheets in {file.name} into csv.")
                write_log("Successfully converted all Excel files into csv.")
            except Exception as e:
                write_log(f"Failed to convert all Excel files into csv due to error '{e}'")
                sys.exit()

        # if user wants to append data, combine all excel sheets and existing flat files into a single csv
        if int(self._append_option) == 1:
            try:
                write_log("User chooses to append data.")
                write_log("Attempting to consolidate all data together into a single table.")

                # read all existing flat files
                flat_files = []
                for ext in ('**/*.csv', '**/*.tsv', '**/*.txt'): flat_files.extend(self._downloaded_folder.glob(ext))
                tables_to_append = []

                # read excels (if any) before appending
                for file in tqdm(files):
                    write_log(f"--Attempting to read all visible sheets in {file.name}.")
                    
                    #check hidden sheets
                    sheets = pd.ExcelFile(file).book.sheets()
                    visible_sheets = [i for i in sheets if i.visibility==0]

                    #read visible sheets only
                    for sheet in visible_sheets: 
                        worksheet_df = pd.read_excel(file, sheet_name=sheet.name) 
                        worksheet_name = str(file.stem) + '_' + str(sheet) 
                        worksheet_df['FileName'] = worksheet_name 
                        tables_to_append.append(worksheet_df)
                    self.move_to_processed(file)
                    write_log(f"--Successfully read all visible sheets in {file.name}.")

                # continue appending flat files
                for flat in tqdm(flat_files): 
                    tables_to_append.append(pd.read_csv(flat, sep = self.detect_file_separator(flat)))
                    self.move_to_processed(flat)
                    write_log(f"Successfully appended in {flat.name}.")

                # save appended data   
                wb_append = pd.concat(tables_to_append, ignore_index=True, sort=False)
                csv_path = self._downloaded_folder / 'consolidated_data.csv'
                if Path.exists(csv_path): Path.unlink(csv_path) #overwrite existing file
                wb_append.to_csv(csv_path, sep="^")
                write_log("Successfully consolidated all data together into a single table.")
            except Exception as e:
                write_log(f"Failed to consolidate all data together into a single table due to error '{e}'.")
                sys.exit()

           
    def upload_extracted_files(self, author="Python"):
        """
        This function uploads all present flat files in downloaded folder onto SQL server using BCP.
            Returns None.
            @author: string to be filled into the log table in SQL, defaulted "Python". 
        """
        files = []
        for ext in ('**/*.csv', '**/*.tsv', '**/*.txt'): files.extend(self._downloaded_folder.glob(ext))
        if len(files) == 0:
            write_log("No files are present in the Downloaded folder.")
            sys.exit()

        try:
            write_log("Attempting to upload all data onto SQL server.")
            self._processed_folder.mkdir(exist_ok=True)
            
            # Create SQLAlchemy session and mapping class for ORM
            try:
                write_log("--Attempting to establish connection to and create log table in SQL server.")
                Base = declarative_base()
                engine = self._trusted_engine if trusted_connection_flag == 1 else self._db_engine
                Session = sessionmaker(bind=engine)
                session = Session()

                # upload log table in SQL
                class Log(Base):
                    __tablename__ = 'tblUploadLog'
                    UploadLogID = Column(Integer, primary_key=True)
                    filename = Column(String(200)) #full file path
                    upload_date = Column(DateTime, default=datetime.utcnow)
                    author = Column(String)
                    SourceTable = Column(String(200)) #file name only without extension
                    num_loaded_rows = Column(BigInteger)
                Base.metadata.create_all(engine)
                write_log("--Successfully established connection to and create log table in SQL server.")
            except Exception as e:
                write_log("--Failed to establish connection to and create log table in SQL server "
                    + f"due to error '{e}'")
                sys.exit()

            # Upload data
            for file in tqdm(files):
                write_log(f"--Attemping to upload {file.name}.")

                # upload column headers
                try:
                    write_log(f"----Attempting to upload column headers for {file.name}.")
                    separator = self.detect_file_separator(file)
                    df = pd.read_csv(file, sep=separator, nrows=1)
                    empty_df = pd.DataFrame(columns=df.columns)
                    table_name = f'{file.stem}'.replace(' ', '_')

                    #create raw schema
                    sp = """IF NOT EXISTS (
                            SELECT  schema_name
                            FROM    information_schema.schemata
                            WHERE   schema_name = 'raw' ) 
                            BEGIN
                            EXEC sp_executesql N'CREATE SCHEMA raw'   
                            END"""
                    session.execute(sp)
                    empty_df.to_sql(table_name, engine, index=False, if_exists='replace', schema='raw')
                    write_log(f"----Successfully uploaded column headers for {file.name}.")
                except Exception as e:
                    write_log(f"----Failed to upload column headers for {file.name} due to error '{e}'. "
                        + "Moving on to the next file.")
                    continue
                
                # upload the rest of the table
                try:
                    write_log(f"----Attemping to upload all data in {file.name}.")
                    if trusted_connection_flag == 1:
                        bcp_query = f"bcp [{self._db_name}].[raw].[{table_name}] in \"{file}\" -F 2 -T -c -S {self._server_name} -t {separator}"    
                    else:
                        bcp_query = f"bcp [{self._db_name}].[raw].[{table_name}] in \"{file}\" -F 2 -U {self._db_username} -P {self._db_password} -c -S {self._server_name} -t {separator}"
                    output = subprocess.run(bcp_query, capture_output=True) 
                    num_lines = re.search(r'(\d+) rows copied', str(output)).group(1) 
                    write_log(f"----Successfully uploaded all data in {file.name}.") 
                except Exception as e: 
                    write_log(f"----Failed to upload all data for {file.name} due to error '{e}'. " 
                        + "Moving on to the next file.") 
                    continue 
                
                # populate upload log table
                try: 
                    write_log(f"----Attemping to populate upload log table for {file.name}.") 
                    log_dict = {'filename': file.name, 
                                'author': author, 
                                'SourceTable': table_name, 
                                'num_loaded_rows': num_lines} 
                    log = Log(**log_dict) 
                    session.add(log) 
                    write_log(f"----Successfully populate upload log table for {file.name}.") 
                except Exception as e:
                    write_log(f"----Failed to populate upload log table for {file.name} due to error '{e}'.") 
                    continue

                self.move_to_processed(file)
                write_log(f"Successfully uploaded {file.name}.")
            
            session.commit()
            write_log("Successfully uploaded all data onto SQL server.")
        except Exception as e:
            write_log(f"Failed to upload all data onto SQL server due to error '{e}'.")
            sys.exit()


    def data_cleansing_master(self):
        """This function governs data cleansing process in this class."""
        if self._blob_flag == 1: self.download_data_blob()
        self.extract_compressed_files()
        self.produce_flat_file()
        self.upload_extracted_files()

