"""
Import Required Libraries
"""
import pickle
import logging
from configparser import ConfigParser


logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

'''
Read Config File
'''
config = ConfigParser()
config.read('config.ini')
MODEL_PATH = config['MODELS']['CLASSIFIER']

"""
Class Name: Model
Parameters: model_path --> Full or relative path in double quotes
Returns: a Loaded Model
"""


class Model:
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None

    def load_model(self):
        try:
            logging.info('Loading the Model')
            self.model = pickle.load(open(self.model_path,'rb'))
            logging.info('Successfully loaded the model')
        except Exception as e:
            logging.error('Could not load the model. Please check the path and model file. Please check the error here')
            logging.error(e)
            return None
        return self.model


if __name__ == "__main__":
    my_model = Model(MODEL_PATH)
    my_loaded_model = my_model.load_model()
    print(my_loaded_model)

