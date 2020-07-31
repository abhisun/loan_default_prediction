"""
This is the entry point to run this project
"""
import logging
from configparser import ConfigParser
from predict_default_probability import DefaultPredictor

'''
Read Config File
'''
config = ConfigParser()
config.read('config.ini')
INPUT_PATH = config['DATA']['PATH']
CLASSIFIER = config['MODELS']['CLASSIFIER']
SCALER = config['MODELS']['SCALER']
TABLE = config['DESTINATION']['TABLE']
OUTPUT_PATH = config['DESTINATION']['PATH']
SERVICE = config['SERVICE']['SERVICENAME']
ENV = config['APP']['ENVIRONMENT']
IDENTIFIERS = config.get('DATA','IDENTIFIERS').split(",")
CATEGORICAL = config.get('DATA', 'CATEGORICAL').split(",")
CUSTOMER = config.items('CUSTOMER')
INSERT_QUERY = config['QUERY']['INSERT']
DROP_QUERY = config['QUERY']['DROP']
CREATE_QUERY = config['QUERY']['CREATE']
USER = config['DATABASE']['USER']
PASSWORD = config['DATABASE']['PASSWORD']
HOST = config['DATABASE']['HOST']
PORT = int(config['DATABASE']['PORT'])
DATABASE = config['DATABASE']['DATABASE']

logging.basicConfig(level=logging.DEBUG)
logging.info("Executing in the standard mode")
logging.info('Started execution of '+SERVICE+' in the '+ENV+' environment')

database_details = {'user': USER, 'password': PASSWORD, 'host': HOST, 'port': PORT, 'database': DATABASE}

prediction_pipeline = DefaultPredictor(CLASSIFIER, IDENTIFIERS, CATEGORICAL, CUSTOMER, SCALER, sql=True,
                                       input_path=INPUT_PATH, output_path=OUTPUT_PATH, table=TABLE,
                                       drop_query=DROP_QUERY, create_query=CREATE_QUERY, insert_query=INSERT_QUERY,
                                       database=database_details)
prediction_pipeline.run_default_pipeline()

logging.info("Execution is over")


