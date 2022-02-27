from pyspark.sql import types
from sandbox.spark_etl.common.data_validator.dtypes_mapper_abstract import dtypes_mapper_abstract

class dtypes_mapper_hive (dtypes_mapper_abstract):
        
    def get_spark_dtypes_mapper(self) -> dict:
        spark_dtypes_mapper_dict = {
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
        return spark_dtypes_mapper_dict
    
