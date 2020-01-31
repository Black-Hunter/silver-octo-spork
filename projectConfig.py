import configparser
from pyspark import  SparkConf

CONFIG_FILE="config.ini"

# Konfiguration für Main und Pyspark
class Config(object):

    def __init__(self):
        print("")

    def getConfig(self):
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_FILE)
        self.name = self.config['Spark']['Name']
        self.host = self.config['Spark']['Threads']
        self.memory = self.config['Spark']['spark.executor.memory']
        confCluster = SparkConf()\
            .setAppName(self.name).setMaster(self.host)
        return confCluster

    def get(self, JOB):
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_FILE)
        return self.config['RUN'][JOB]