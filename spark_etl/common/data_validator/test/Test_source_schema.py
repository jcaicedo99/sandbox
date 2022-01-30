import findspark
findspark.init()

from pyspark.sql import types
from sandbox.spark_etl.common.data_validator.source_schema import source_schema


class Test_source_schema(object):
    
    @staticmethod
    def test_is_valid_list_True():
        
        is_valid,invalid_element = source_schema([]).is_valid_list([{"column_name":"col_name","required":True,"data_type":"integer","format":""}])
        
        expected_result,expected_list = True,[]
        
        assert (expected_result,expected_list) == (is_valid,invalid_element), "test_is_valid_list_True Failed"
        
    @staticmethod
    def test_is_valid_list_False():   
        is_valid,invalid_elements = source_schema([]).is_valid_list([{"column_name":"col_name","required":True,"data_typ":"integer","format":""}])
        
        expected_result,expected_element = False,{"column_name":"col_name","required":True,"data_typ":"integer","format":""}
        
        assert (expected_result == is_valid) and (expected_element == invalid_elements[0]), "test_is_valid_list_False Failed"   
