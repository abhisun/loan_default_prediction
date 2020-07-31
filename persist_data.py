from configparser import ConfigParser
import logging
from datetime import date
import pandas as pd
import mysql.connector

'''
This python script will persist the output data to the destination we provide.
example - Local file storage, HDFS file storage or SQL Database
'''

logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

'''
Read config variables
'''
config = ConfigParser()
config.read('config.ini')
TABLE = config['DESTINATION']['TABLE']
PATH = config['DESTINATION']['PATH']
USER = config['DATABASE']['USER']
PASSWORD = config['DATABASE']['PASSWORD']
HOST = config['DATABASE']['HOST']
PORT = int(config['DATABASE']['PORT'])
DATABASE = config['DATABASE']['DATABASE']
INSERT_QUERY = config['QUERY']['INSERT']
DROP_QUERY = config['QUERY']['DROP']
CREATE_QUERY = config['QUERY']['CREATE']


class DataPersister:
    def __init__(self, data, db=None, table=None, create_query=None, drop_query=None, insert_query=None, path=None):
        self.db = db
        self.connector = None
        self.table = table
        self.data = data
        self.path = path
        self.drop_query = drop_query
        self.create_query = create_query
        self.insert_query = insert_query
        self.cursor = None

    def connect_to_db(self):
        try:
            logging.info("Establishing connection with DB")
            self.connector = mysql.connector.connect(user=self.db.get('user'),
                                                     password=self.db.get('password'),
                                                     host=self.db.get('host'),
                                                     port=self.db.get('port'),
                                                     database=self.db.get('database')
                                                     )
            self.cursor = self.connector.cursor()
            logging.info("Successfully connected to DB")
        except Exception as e:
            logging.error("Failed to connect to the DB. Please check the error below")
            logging.error(e)
            return 1
        return 0

    def save_to_file(self):
        try:
            logging.info("Saving to File")
            file_path = self.path + str(date.today()) + ".csv"
            self.data.to_csv(file_path)
            logging.info("Successfully saved the file at the provided path")
        except Exception as e:
            logging.error("Failed to save the file to the path. Please check the error below")
            logging.error(e)
            return 1
        return 0

    def save_to_table(self):
        try:
            self.create_query = open(self.create_query, 'r').read()
            self.drop_query = open(self.drop_query, 'r').read()
            self.insert_query = open(self.insert_query, 'r').read()

            logging.info("Saving to Table")
            cols = "`,`".join([str(i) for i in self.data.columns.tolist()])

            self.cursor.execute(self.drop_query.format(db=self.db.get('database'), table=self.table))
            #self.cursor.execute("Drop Table if Exists hackathon_demo.bank_loan_default_prediction")
            self.connector.commit()

            self.cursor.execute(self.create_query.format(db=self.db.get('database'), table=self.table))
            self.connector.commit()

            for i, row in self.data.iterrows():
                #sql = "INSERT INTO `"+self.table+"` (`"+cols+"`) VALUES ("+"%s," * (len(row) - 1) + "%s)"
                sql = self.insert_query.format(table=self.table, columns=cols)
                self.cursor.execute(sql, tuple(row))
            self.connector.commit()
            logging.info("Successfully saved to table")
        except Exception as e:
            logging.error("Failed to save to Table")
            logging.error(e)
            return 1
        return 0

    def persist(self):
        try:
            logging.info("Started Persisting the data")
            if self.db:
                connection_result = self.connect_to_db()
                if connection_result != 0:
                    logging.error("Failed to Persist")
                    return -1
                save_result = self.save_to_table()
                if save_result != 0:
                    logging.error("Failed to Persist")
                    return -1
            else:
                save_result = self.save_to_file()
                if save_result != 0:
                    logging.error("Failed to Persist")
                    return -1
        except Exception as e:
            logging.error("Failed to Persist to Table. Check error below")
            logging.error(e)
            return -1
        return 0


if __name__ == "__main__":
    DATA = "data\\insights\\2020-07-31.csv"
    data_ = pd.read_csv(DATA)
    database_details = {'user': USER, 'password': PASSWORD, 'host': HOST, 'port': PORT, 'database': DATABASE}
    persister = DataPersister(data_, db=database_details, table=TABLE, path=PATH, create_query=CREATE_QUERY,
                              drop_query=DROP_QUERY, insert_query=INSERT_QUERY)
    persister.persist()
    print(config.items('DATABASE'))





