import findspark
findspark.init()

from pyspark.sql import types
from sandbox.spark_etl.common.data_validator.source_schema import source_schema
from sandbox.spark_etl.common.data_validator.dtype_mapper_hive import dtypes_mapper_hive

class Test_source_schema(object):
    
    def __init__(self):
        self.raw_schema_MD = [
            {
            "column_name" : "COLUMN_1",
            "required" : True,
            "data_type" : "varchar(10)",
            "format" : ""
            },
            {
                "column_name" : "COLUMN_2",
                "required" : False,
                "data_type" : "decimal(10,4)",
                "format" : ""
            },
            {
                "column_name" : "COLUMN_2",
                "required" : False,
                "data_type" : "timestamp",
                "format" : ""
            }        
        ]
        
        # self.expected_schema_extended = 
    
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
    
    @staticmethod
    def test_get_sourceschema_extended_hivedtypes(self):
        
        schema_extended = source_schema(list_of_dict=self.raw_schema_MD,dtype_translator=dtypes_mapper_hive())
        print(schema_extended.json())
        
        assert True
        