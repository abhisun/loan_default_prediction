"""
Import the Libraries
"""

import logging
from configparser import ConfigParser
import numpy as np

'''
This script combines the input dataset with results before persisting it in SQL
'''

logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

'''
Read Config File
'''
config = ConfigParser()
config.read('config.ini')
INPUT_DATA = config['DATA']['PATH']

'''
class: PostProcessor
Parameters: data -> Pandas DataFrame
            predictions -> Numpy array
Returns: Pandas DataFrame -> concats the input data
'''


class PostProcessor:
    def __init__(self, data, predictions, tiers):
        self.data = data
        self.predictions = predictions
        self.tiers = tiers

    def combine_data(self):
        try:
            logging.info("Concatenating the predictions and tiers with input dataset")
            self.data['default_probability'] = self.predictions
            self.data['Customer_tiers'] = self.tiers
            logging.info("Successfully concatenated the results")
        except Exception as e:
            logging.error("Failed to concat the data together")
            logging.error(e)
            return None
        return self.data


if __name__ == "__main__":
    from ingest_data import DataLoader
    input_data = DataLoader(file_path=INPUT_DATA).load_data()
    predictions_ = np.random.uniform(0, 1, size=input_data.shape[0])
    tiers_ = ['A']*200
    processor = PostProcessor(input_data, predictions_, tiers_)
    prediction_data = processor.combine_data()
    print(prediction_data.head())




