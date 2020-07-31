"""
This script is for ingesting the data from the source to memory
"""
import pandas as pd
import logging
from configparser import ConfigParser

logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

'''
Read Config File
'''
config = ConfigParser()
config.read('config.ini')
PATH = config['DATA']['PATH']


"""
Class Name: DataLoader
Parameters: database = database connectivity details
            query = SQL Query to extract the data
            file_path = path a saved data file (only works when database is not given)
Returns: a Loaded Model
"""


class DataLoader:
    def __init__(self, database=None, query=None, file_path=None):
        self.database = database
        self.query = query
        self.data = None
        self.file_path = file_path

    def load_data(self):
        try:
            logging.info('Loading the Data')
            if self.database and self.query:
                logging.info('Loading the Data From database')
                pass  # TODO complete the SQL connection here
                logging.info('Successfully loaded the data from the database')
            else:
                logging.info('Loading the data from csv')
                self.data = pd.read_csv(self.file_path)
                logging.info('Successfully loaded the data from CSV')
        except Exception as e:
            logging.error('There some problem with loading the data. Please check the error below.')
            logging.error(e)
            return None
        return self.data


if __name__ == "__main__":
    loader = DataLoader(file_path=PATH)
    my_data = loader.load_data()
    print(my_data)

