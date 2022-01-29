import findspark
findspark.init()

from pyspark.sql import types
from sandbox.spark_etl.common.data_validator.dtypes_mapper import dtypes_mapper


class Test_sparkconnection(object):
    
    @staticmethod
    def test_varchar_dtype():
        
        dict_translation = dtypes_mapper.translate_dtype_to_sparkdtype("varchar(10)")
        
        expected_result = {"spark_dtype":types.StringType(),"source_dtype" :"varchar(10)","precision" :'10'}
        
        assert dict_translation == expected_result
        
    @staticmethod
    def test_decimal_dtype():
        
        dict_translation = dtypes_mapper.translate_dtype_to_sparkdtype("decimal(10,2)")
        
        expected_result = {"spark_dtype":types.DecimalType(10,2),"source_dtype" :"decimal(10,2)","precision" :'10',"scale":'2'}
        
        assert dict_translation == expected_result   
        
    @staticmethod
    def test_timestamp_dtype():
        
        dict_translation = dtypes_mapper.translate_dtype_to_sparkdtype(" timestamp")
        
        expected_result = {"spark_dtype":types.TimestampType(),"source_dtype" :"timestamp","precision" :None}
        
        assert dict_translation == expected_result      