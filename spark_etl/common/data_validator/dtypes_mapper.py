import findspark
findspark.init()

from typing import Tuple
from pyspark.sql import types
import re

class dtypes_mapper(object):
    def __init__(self):
        pass
    
    @staticmethod
    def spark_dtypes_mapper() -> dict:
        spark_dtypes_mapper = {
        "varchar": types.StringType(),
        "varchar2": types.StringType(),
        "char": types.StringType(),
        "string": types.StringType(),
        "datetime": types.TimestampType(),
        "date": types.DateType(),
        "int": types.IntegerType(),
        "integer": types.IntegerType(),
        "long": types.LongType(),
        "bigint": types.LongType(),
        "float": types.FloatType(),
        "timestamp": types.TimestampType(),
        "double": types.DoubleType()
        }
        
        """decimal datatype is mapped in another method"""
        return spark_dtypes_mapper
    
    
    @staticmethod
    def __is_datatype_with_precision(raw_dtype : str) -> bool:
        return any (raw_dtype.startswith(c) for c in "varchar,varchar2,char,decimal".split(","))
    
    @staticmethod
    def __is_decimaltype(input_raw_dtype : str) -> bool:
        return input_raw_dtype.startswith("decimal")
    
    @staticmethod
    def __cleanup_raw_dtype(raw_dtype : str) -> str:
        return raw_dtype.replace(" ","").lower()

    @staticmethod
    def __get_spark_decimal_type(raw_dtype:str) -> dict:
        regexp=r"decimal(?=\(([0-9]{1,2}),([0-9]{1,2})\))"
        re_result = re.match(regexp,raw_dtype)
        
        if re_result is None:
           msg="Error: Invalid syntax for datatype {0}".format(raw_dtype) 
           raise ValueError(msg)
       
        precision, scale = re_result.groups()    
        try:
           spark_decimal_dtype = types.DecimalType(int(precision),int(scale))
        except:
           msg = "Error: Invalid syntax for Decimal dtype {0}".format(raw_dtype)
           raise ValueError(msg)
        
        return {"spark_dtype":spark_decimal_dtype,"source_dtype" :raw_dtype,"precision" :precision,"scale":scale}
        

    @staticmethod
    def __parse_dtypes(raw_dtype : str) -> dict:        
        raw_dtype_cleanedup=dtypes_mapper.__cleanup_raw_dtype(raw_dtype)
        
        if raw_dtype.startswith("decimal("):
           return dtypes_mapper.__get_spark_decimal_type(raw_dtype) 
        
        regexp=r"(\w+)(?=\(([0-9]{1,4})\))?"
        re_result = re.match(regexp,raw_dtype_cleanedup)
        
        if re_result is None:
            msg="ERROR: Invalid syntax for datatype {0}".format(raw_dtype)
            raise ValueError(msg)
        
        dtype,precision = re_result.groups()
        
        assert dtype in dtypes_mapper.spark_dtypes_mapper().keys(), "DataType {0} not found in the spark dtype mapper".format(dtype)
        #if datatype has no precision second value below is None
        return {"spark_dtype":dtypes_mapper.spark_dtypes_mapper()[dtype],"source_dtype" :raw_dtype_cleanedup,"precision" :precision}
    
    
    def translate_dtype_to_sparkdtype(raw_dtype : str) -> dict:
        """return dictionary {spark_dtype:"","source_dtype":"","precision":"" """
        return dtypes_mapper.__parse_dtypes(raw_dtype)
    
    
    
# print( dtypes_mapper.translate_dtype_to_sparkdtype("varchar(10)")  )      
# print( dtypes_mapper.translate_dtype_to_sparkdtype("decimal(10,2)") )
# print (dtypes_mapper.translate_dtype_to_sparkdtype("date") )
            
        
        
        