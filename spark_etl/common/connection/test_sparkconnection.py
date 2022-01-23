import findspark
findspark.init()

from sandbox.spark_etl.common.test_utils import test_utils
from sandbox.spark_etl.common.connection.sparkconnection import sparkconnection

class test_sparkconnection(object):
    
#    def __init__(self):
#        pass
#     #    test_utils.add_project_home()
#        test_utils.setsparkenv()
       
       
   def test_local_sparkconnection():

       master="local[*]"
       session_name="test_local_sparkconnection"
       settings = {"spark.executor.cores": 2,"spark.executor.memory": "2g"}
       
       ss = sparkconnection().get_session(master=master,session_name=session_name,connection_settings=settings) 
       
       with ss as spark_ss:   
          print(spark_ss.range(1).collect()[0])        
          assert spark_ss.range(1).collect()[0]["id"] == 0, "test_local_sparkconnection failed"
       
       
       
       
test_sparkconnection.test_local_sparkconnection()      