

from pyspark.sql import functions as F, SparkSession
from pyspark import SparkContext, SparkConf

class sparkconnection(object):

    def __init__(self):
        pass
    
    @staticmethod
    def __getSparkConf(spark_settings : dict) -> SparkConf:
        spark_cnf = SparkConf()
        for parameter,value in spark_settings.items():
            spark_cnf.set(parameter,value)

        return spark_cnf    
    

    def get_session(self,session_name : str, master : str,connection_settings : dict):

        spark_config = sparkconnection.__getSparkConf(spark_settings=connection_settings)
        spark_context = SparkContext(master = master, appName = session_name,conf=spark_config)

        session = SparkSession(spark_context)

        return session


