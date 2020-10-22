"""
Sun J, 2020/03/23:
    1. Modules required for this tool are listed in requirements.txt 
    2. To install modules in a windows system, use "pip install -r requirements.txt" after configuring environment.
"""
import config
import helper
import sys
from pathlib import Path

if __name__ == "__main__":
    try:
        kwargs = dict(blob_container_name = config.blob_container_name,
                    blob_access = config.blob_access,
                    blob_folder_name = config.blob_folder_name,
                    blob_archive_name = config.blob_archive_name,
                    blob_flag = config.blob_flag,
                    trusted_engine = config.trusted_engine,
                    db_engine = config.db_engine,
                    server_name = config.server_name,
                    db_name = config.db_name,
                    db_username = config.db_username,
                    db_password = config.db_password,
                    append_option = config.append_option,
                    downloaded_folder = Path.cwd()/'Downloaded',
                    processed_folder = Path.cwd()/'Processed')

        helper.connection_check(**kwargs).connection_check_master()
        helper.data_cleansing(**kwargs).data_cleansing_master()

    except ImportError as e: print("One or more required modules is not installed: " + e.name); sys.exit()
    except ModuleNotFoundError as e: print("One or more required modules is not installed: " + e.name); sys.exit()