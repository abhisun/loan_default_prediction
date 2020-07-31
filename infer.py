"""
This module will pre-process the input data
"""
import pandas as pd
from load_model import Model
from configparser import ConfigParser
import logging


logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

'''
Read the config File
'''
config = ConfigParser()
config.read('config.ini')
CLASSIFIER = config['MODELS']['CLASSIFIER']

'''
class: Classifier
Parameters: data --> Numpy ndarray of the transformed data
            model_path --> String value; full or relative path to the saved model
Returns: Numpy array of the default Probabilities
'''


class Classifier:
    def __init__(self, data, model_path):
        self.data = data
        self.model_path = model_path
        self.model = None
        self.probabilities = None

    def load_model(self):
        try:
            self.model = Model(self.model_path).load_model()
            logging.info("The model successfully loaded")
        except Exception as e:
            logging.error("The model Failed to load")
            return 1
        return 0

    def get_probabilities(self):
        try:
            self.probabilities = self.model.predict_proba(self.data)[:,1]
            logging.info("Successfully inferred the probabilities")
        except Exception as e:
            logging.error("The Probabilities could not be inferred")
            return 1
        return 0

    def infer_data(self):
        try:
            logging.info('Starting inference')
            model_result = self.load_model()
            if model_result != 0:
                logging.error('Inference failed. Please check the error messages')
                return None
            probability_result = self.get_probabilities()
            if probability_result != 0:
                logging.error('Inference failed. Please check the error messages')
                return None
            logging.info('Successfully Inferred')
        except Exception as e:
            logging.error('Inference failed. Please check the error messages')
            return None
        return self.probabilities


if __name__ == "__main__":
    import prepare_input_data as pid
    DATA = config['DATA']['PATH']
    SCALER = config['MODELS']['SCALER']
    IDENTIFIERS = ['customer_id', 'loan_id']
    data_ = pd.read_csv(DATA)
    _, input_data = pid.DataPreprocess(data_, SCALER, IDENTIFIERS).prepare_data()
    print(CLASSIFIER)
    classifier_model = Classifier(input_data, CLASSIFIER)
    probabilities = classifier_model.infer_data()
    print(probabilities)

