import logging
from configparser import ConfigParser
import pandas as pd
from matplotlib import pyplot as plt

"""
This module calculates customer tier using standard rules and points

Rules:
Points based system to divide the customers into three different groups

past Loans Matured:
if > 2: then points = 10
elif > 0: then points = 5
else: points = 0

Non-Payments:
if == 0: then points = 10
elif < 2: then points = 5
else: points = 0

Savings:
if savings > 100000: then points = 10
elif savings > 50000: then points = 5
else: points = 0
"""

logging.basicConfig(level=logging.DEBUG)
logging.info('Executing the script as a standalone')

'''
Read Config File
'''
config = ConfigParser()
config.read('config.ini')
CUSTOMER = config.items('CUSTOMER')
DATA = config['DATA']['PATH']


class TierClassifier:
    def __init__(self, data, rules):
        self.data = data
        self.rules = rules

    def apply_rule(self, val, x, rule):
        try:
            high = rule[0]
            low = rule[1]
            if val == 'less':
                if x <= low:
                    return 10
                elif x <= high:
                    return 5
                else:
                    return 0
            else:
                if x > high:
                    return 10
                elif x > low:
                    return 5
                else:
                    return 0
        except Exception as e:
            logging.error("Failed to Apply rule. check here")
            logging.error(e)

    def get_points(self, x):
        points = 0
        try:
            for rule in self.rules:
                col_name = rule[0]
                high, low, value = rule[1].split(",")
                points += self.apply_rule(value, x[col_name], [int(high), int(low)])
            return points
        except Exception as e:
            logging.error("Failed to get Points")
            logging.error(e)

    def get_tier(self, x):
        try:
            points = self.get_points(x)
            if points >= 22:
                return 'A'
            elif points >= 15:
                return 'B'
            else:
                return 'C'
        except Exception as e:
            logging.error("Failed to get tier")
            logging.error(e)

    def rule_engine(self):
        try:
            logging.info("Applying the rule engine to get tiers")
            tiers = self.data.apply(self.get_tier, axis=1)
            return tiers
        except Exception as e:
            logging.error("Failed to apply rules engine to get tiers")
            logging.error(e)
            return

if __name__ == "__main__":
    data_ = pd.read_csv(DATA)
    tier_classifier = TierClassifier(data_, CUSTOMER)
    tier = tier_classifier.rule_engine()
    plt.hist(tier)
    plt.show()




