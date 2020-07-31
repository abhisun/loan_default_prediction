"""
This module will pre-process the input data
"""
import pandas as pd
from load_model import Model
from configparser import ConfigParser
import logging


logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

config = ConfigParser()
config.read('config.ini')
IDENTIFIERS = config.get('DATA', 'IDENTIFIERS').split(",")
CATEGORICAL = config.get('DATA', 'CATEGORICAL').split(",")
SCALER = config['MODELS']['SCALER']
DATA = config['DATA']['PATH']

'''
class: DataPreprocess
Parameters: Data-> pandas Dataframe
            scaler_path -> string value; full or relative path
            identifiers -> list object ['col1', 'col2']
Returns: Original data frame, transformed data frame
'''


class DataPreprocessor:
    def __init__(self, data, scaler_path, identifiers, categorical):
        self.data = data
        self.transformed_data = data.copy()
        self.scaler_path = scaler_path
        self.identifiers = identifiers
        self.categorical = categorical

    def handle_missing_data(self):  # TODO: Implement this function in the future
        try:
            logging.info('Handling missing Data')
            pass
            logging.info('successfully handled missing data')
        except Exception as e:
            logging.error('Handling missing data failed with error:')
            logging.error(e)
            return 1
        return 0

    def handle_invalid_data(self):  # TODO: Implement this function in the future
        try:
            logging.info('Handling invalid Data')
            pass
            logging.info('successfully handled invalid data')
        except Exception as e:
            logging.error('Handling invalid data failed with error:')
            logging.error(e)
            return 1
        return 0

    def handle_categorical_data(self):  # TODO: Implement a more dynamic function later
        try:
            logging.info('Handling discrete Data')
            for col in self.categorical:
                if col == 'loan_type':
                    self.transformed_data[col] = self.transformed_data[col].apply(lambda x: 1 if x =='Home' else 0)
                elif col == 'gender':
                    self.transformed_data[col] = self.transformed_data[col].apply(lambda x: 1 if x =='Male' else 0)
                else:
                    self.transformed_data[col] = self.transformed_data[col].apply(lambda x: 1 if x =='Yes' else 0)
            logging.info('successfully handled discrete data')
        except Exception as e:
            logging.error('Handling discrete data failed with error:')
            logging.error(e)
            return 1
        return 0

    def remove_identifiers(self):
        try:
            logging.info('removing identifier Data')
            self.transformed_data.drop(self.identifiers, axis=1, inplace=True)
            logging.info('successfully removed identifier data')
        except Exception as e:
            logging.error('removing identifier data failed with error:')
            logging.error(e)
            return 1
        return 0

    def normalize_data(self):
        try:
            logging.info('Normalizing Data')
            scaler = Model(self.scaler_path)
            scaler_loaded = scaler.load_model()
            self.transformed_data = scaler_loaded.transform(self.transformed_data)
            logging.info('successfully  normalized data')
        except Exception as e:
            logging.error('normalizing data failed with error:')
            logging.error(e)
            return 1
        return 0

    def prepare_data(self):
        try:
            missing_data_result = self.handle_missing_data()
            if missing_data_result != 0:
                logging.error('The Data Preparation has failed. Please check the error messages')
                return None
            invalid_data_result = self.handle_invalid_data()
            if invalid_data_result != 0:
                logging.error('The Data Preparation has failed. Please check the error messages')
                return None
            categorical_data_result = self.handle_categorical_data()
            if categorical_data_result != 0:
                logging.error('The Data Preparation has failed. Please check the error messages')
                return None
            remove_identifier_result = self.remove_identifiers()
            if remove_identifier_result != 0:
                logging.error('The Data Preparation has failed. Please check the error messages')
                return None
            normalize_result = self.normalize_data()
            if normalize_result != 0:
                logging.error('The Data Preparation has failed. Please check the error messages')
                return None

        except Exception as e:
            logging.error('The Data Preparation has failed. Please check the error messages')
            logging.error(e)
            return None
        logging.info('Successfully Prepared the data')
        return self.transformed_data


if __name__ == "__main__":
    data_ = pd.read_csv(DATA)
    preprocessor = DataPreprocessor(data_, SCALER, IDENTIFIERS)
    transform_data = preprocessor.prepare_data()
    print(transform_data.shape)


