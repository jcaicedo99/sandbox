import findspark
findspark.init()

from pyspark.sql import types
from sandbox.spark_etl.common.data_validator.source_schema import source_schema
from sandbox.spark_etl.common.data_validator.dtype_mapper_hive import dtypes_mapper_hive
import  pytest

class Test_source_schema_hive(object):    
    __raw_schema_MD = [
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
    
    __expected_result = [ 
                            {'metadata': {'datatype_precision': '10', 'datatype_scale': None, 'format': '', 'source_datatype': 'varchar(10)', 'spark_datatype': 'string'}, 'name': 'COLUMN_1', 'nullable': False, 'type': 'string'},
                            {'metadata': {'datatype_precision': '10', 'datatype_scale': '4', 'format': '', 'source_datatype': 'decimal(10,4)', 'spark_datatype': 'decimal(10,4)'}, 'name': 'COLUMN_2', 'nullable': True, 'type': 'decimal(10,4)'},
                            {'metadata': {'datatype_precision': None, 'datatype_scale': None, 'format': '', 'source_datatype': 'timestamp', 'spark_datatype': 'timestamp'}, 'name': 'COLUMN_2', 'nullable': True, 'type': 'timestamp'}
    ]
    
    __expected_result_all_strings = [ 
                            {'metadata': {'datatype_precision': '10', 'datatype_scale': None, 'format': '', 'source_datatype': 'varchar(10)', 'spark_datatype': 'string'}, 'name': 'COLUMN_1', 'nullable': False, 'type': 'string'},
                            {'metadata': {'datatype_precision': '10', 'datatype_scale': '4', 'format': '', 'source_datatype': 'decimal(10,4)', 'spark_datatype': 'decimal(10,4)'}, 'name': 'COLUMN_2', 'nullable': True, 'type': 'string'},
                            {'metadata': {'datatype_precision': None, 'datatype_scale': None, 'format': '', 'source_datatype': 'timestamp', 'spark_datatype': 'timestamp'}, 'name': 'COLUMN_2', 'nullable': True, 'type': 'string'}
    ]
    
    __schema_object = source_schema(list_of_dict=__raw_schema_MD,dtype_translator=dtypes_mapper_hive())
        
    #TODO implementing testing of source_schema object creation
        
    
    def test_is_raw_schema_valid_True(self):
        
        is_valid,invalid_element = self.__schema_object.is_valid_list(self.__raw_schema_MD)
        
        expected_result,expected_list = True,[]
        
        assert (expected_result,expected_list) == (is_valid,invalid_element), "test_is_valid_list_True Failed"
        
    def test_is_raw_schemavalid_valid_False(self):   
        is_valid,invalid_elements = self.__schema_object.is_valid_list([{"column_name":"col_name","required":True,"data_typ":"integer","format":""}])
        
        expected_result,expected_element = False,{"column_name":"col_name","required":True,"data_typ":"integer","format":""}
        
        assert (expected_result == is_valid) and (expected_element == invalid_elements[0]), "test_is_valid_list_False Failed"   
    
    
    def test_get_sourceschema_extended_hivedtypes(self):
        
        from json import dumps,loads
        
        
        def format_json(output_json : dict ) -> dict:
            return dumps( output_json["fields"] )
                            
        schema_extended = self.__schema_object.get_struct_schema_with_sparkdtypes(overwrite_spark_dtype_to_string_dtype_flag=False)
        
        #compare side by side dictionay elements from each list    
        assert dumps(self.__expected_result) == format_json(loads(schema_extended.json())) , "test_get_sourceschema_extended_hivedtypes Failed"
        
    def test_get_sourceschema_extended_hivedtypes_all_strings(self):
        
        from json import dumps,loads
                
        def format_json(output_json : dict ) -> dict:
            return dumps( output_json["fields"] )
                            
        schema_extended = self.__schema_object.get_struct_schema_with_sparkdtypes(overwrite_spark_dtype_to_string_dtype_flag=True)
        print("source",dumps(self.__expected_result_all_strings) )
        print("result",format_json(loads(schema_extended.json())))
        #compare side by side dictionay elements from each list    
        assert dumps(self.__expected_result_all_strings) == format_json(loads(schema_extended.json())) , "test_get_sourceschema_extended_hivedtypes Failed"
        