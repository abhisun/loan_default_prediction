import logging
from configparser import ConfigParser
from ingest_data import DataLoader
from prepare_input_data import DataPreprocessor
from infer import Classifier
from prepare_output_data import PostProcessor
from persist_data import DataPersister
from get_customer_tier import TierClassifier


"""
This script performs Data Ingestion to Insights Persisting by calling all the logic in Order
"""

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
logging.info("Executing in the standalone mode")
logging.info('Started execution of '+SERVICE+' in the '+ENV+' environment')

"""
class: DefaultPredictor
Parameters: 
Returns: None
Calls all operations in order
Ingestion --> Data Preparation --> Model Load --> Infer --> Post Process --> Persist results
"""


class DefaultPredictor:
    def __init__(self,  model_path, identifiers, categorical, customer_rules, scaler_path=None, sql=True, database=None,
                 input_path=None, output_path=None, load_query=None, table=None, drop_query=None, create_query=None,
                 insert_query=None):
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.sql = sql
        self.database = database
        self.input_path = input_path
        self.output_path = output_path
        self.load_query = load_query
        self.drop_query = drop_query
        self.create_query = create_query
        self.insert_query = insert_query
        self.table = table
        self.identifiers = identifiers
        self.categorical = categorical
        self.customer_rules = customer_rules
        self.input_data = None
        self.transformed_data = None
        self.predictions = None
        self.customer_tiers = None
        self.output_data = None

    def validate_params(self):
        try:
            logging.info("Validating the input parameters")
            if self.sql:
                if self.database is None:
                    logging.error("If SQL is TRUE then valid Database details should be provided")
                    return -1
                #elif self.load_query is None:
                #    logging.error("If SQL is TRUE then a valid load query should be provided")
                #    return -1
                elif self.insert_query is None:
                    logging.error("If SQL is TRUE then a valid save query should be provided")
                    return -1
                else:
                    pass
            else:
                if self.input_path is None:
                    logging.error("If SQL is False then a valid input path should be provided")
                    return -1
                elif self.output_path is None:
                    logging.error("If SQL is False then a Valid output path should be given")
                    return -1
                else:
                    pass
            logging.info("Successfully validated the input Parameters")
        except Exception as e:
            logging.error("Failed to Validate the input Parameters")
            return -1
        return 0

    def run_ingest_data(self):
        try:
            logging.info("Calling the method to ingest data")
            data_loader = DataLoader(database=self.database, query=self.load_query, file_path=self.input_path)
            self.input_data = data_loader.load_data()
        except Exception as e:
            logging.error("Failed to ingest data")
            return -1
        return 0

    def run_prepare_input_data(self):
        try:
            logging.info("Calling the method to prepare input")
            data_preprocessor = DataPreprocessor(data=self.input_data, scaler_path=self.scaler_path,
                                                 identifiers=self.identifiers, categorical=self.categorical)
            self.transformed_data = data_preprocessor.prepare_data()
        except Exception as e:
            logging.error("Failed to prepare data")
            logging.error(e)
            return -1
        return 0

    def run_infer(self):
        try:
            logging.info("Calling the method to infer data")
            classifier = Classifier(data=self.transformed_data, model_path=self.model_path)
            self.predictions = classifier.infer_data()
        except Exception as e:
            logging.error("Failed to Infer data")
            return -1
        return 0

    def run_get_customer_tier(self):
        try:
            logging.info("calling the method to generate customer tier")
            tier_classifier = TierClassifier(self.input_data, self.customer_rules)
            self.customer_tiers = tier_classifier.rule_engine()
        except Exception as e:
            logging.error("Failed to get customer tiers")
            return -1
        return 0

    def run_prepare_output_data(self):
        try:
            logging.info("Calling the method to prepare outputs")
            post_processor = PostProcessor(data=self.input_data, predictions=self.predictions, tiers=self.customer_tiers)
            self.output_data = post_processor.combine_data()
        except Exception as e:
            logging.error("Failed to prepare outputs")
            return 1
        return 0

    def run_persist_data(self):
        try:
            logging.info("Calling the method persist data")
            data_persister = DataPersister(data=self.output_data, db=self.database,
                                           table=self.table, path=self.output_path,
                                           create_query=self.create_query,
                                           drop_query=self.drop_query,
                                           insert_query=self.insert_query)
            persist_results = data_persister.persist()
            if persist_results != 0:
                logging.error("Failed in persisting")
                return -1
            return 0
        except Exception as e:
            logging.error("Failed in persisting")
            return -1

    def run_default_pipeline(self):
        try:
            logging.info("Running the pipeline")
            validate_results = self.validate_params()
            if validate_results != 0:
                logging.error("Failed to execute the pipeline at validating parameters")
                return
            ingest_results = self.run_ingest_data()
            if ingest_results != 0:
                logging.error("Failed to execute the pipeline at ingesting data")
                return
            prepare_input_results = self.run_prepare_input_data()
            if prepare_input_results != 0:
                logging.error("Failed to execute the pipeline at preparing inputs")
                return
            infer_results = self.run_infer()
            if infer_results != 0:
                logging.error("Failed to execute the pipeline at inferring data")
                return
            tier_results = self.run_get_customer_tier()
            if tier_results != 0:
                logging.error("Failed to execute the pipeline at getting tiers")
                return
            prepare_output_results = self.run_prepare_output_data()
            if prepare_output_results != 0:
                logging.error("Failed to execute the pipeline at preparing output data")
                return
            persist_results = self.run_persist_data()
            if persist_results != 0:
                logging.error("Failed to execute the pipeline at persisting data")
                return
            logging.info("Successfully executed the pipeline")
        except Exception as e:
            logging.error("Failed to execute the pipeline. Check the error below")
            logging.error(e)
        return


if __name__ == "__main__":
    database_details = {'user': USER, 'password': PASSWORD, 'host': HOST, 'port': PORT, 'database': DATABASE}
    prediction_pipeline = DefaultPredictor(CLASSIFIER, IDENTIFIERS, CATEGORICAL, CUSTOMER, SCALER, sql=True,
                                           input_path=INPUT_PATH, output_path=OUTPUT_PATH, table=TABLE,
                                           drop_query=DROP_QUERY, create_query=CREATE_QUERY, insert_query=INSERT_QUERY,
                                           database=database_details)
    prediction_pipeline.run_default_pipeline()








