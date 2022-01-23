import findspark
findspark.init()

from sandbox.spark_etl.common.connection.sparkconnection import sparkconnection

class Test_sparkconnection(object):
    
   @staticmethod 
   def test_local_sparkconnection():

       master="local[*]"
       session_name="test_local_sparkconnection"
       settings = {"spark.executor.cores": 2,"spark.executor.memory": "2g"}
       
       ss = sparkconnection().get_session(master=master,session_name=session_name,connection_settings=settings) 
       
       with ss as spark_ss:   
          print(spark_ss.range(1).collect()[0])        
          assert spark_ss.range(1).collect()[0]["id"] == 0, "test_local_sparkconnection failed"
       