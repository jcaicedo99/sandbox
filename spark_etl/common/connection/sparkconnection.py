import findspark
findspark.init() 

from pyspark.sql import functions as F, SparkSession
from pyspark import SparkContext, SparkConf

class sparkconnection(object):

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


scon = sparkconnection()

ss = scon.get_session(master="local[*]",session_name="mytest",connection_settings={"spark.executor.cores": 2,"spark.executor.memory": "2g"})

print(ss.range(1).collect())
print(ss.version)


ss.stop()



