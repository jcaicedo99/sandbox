from abc import ABC, abstractmethod
import re
from pyspark.sql import types


class dtypes_mapper_abstract(ABC):
    
    @abstractmethod
    def get_spark_dtypes_mapper(self) -> dict:
        pass
        
    def __cleanup_raw_dtype(self,raw_dtype : str) -> str:
        return raw_dtype.replace(" ","").lower()
    

    def __is_decimaltype(self,raw_dtype : str) -> bool:
        return raw_dtype.startswith("decimal")
    
    def __get_spark_decimal_type(self,raw_dtype:str) -> dict:
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
    
    
    def __parse_dtype(self,raw_dtype : str) -> dict:
        "parse datatype with precision and return dictionary metadata"
        
        regexp=r"(\w+)(?=\(([0-9]{1,4})\))?"
        re_result = re.match(regexp,raw_dtype)
        
        if re_result is None:
            msg="ERROR: Invalid syntax for datatype {0}".format(raw_dtype)
            raise ValueError(msg)
        
        dtype_parsed ,precision_parsed = re_result.groups()
        
        assert dtype_parsed in self.get_spark_dtypes_mapper().keys(), "DataType {0} not found in the spark dtype mapper".format(dtype_parsed)
        #if datatype has no precision second value below is None
        return {"spark_dtype":self.get_spark_dtypes_mapper()[dtype_parsed],"source_dtype" :raw_dtype,"precision" :precision_parsed}
        
    
    def translate_dtype_to_sparkdtype(self,raw_dtype : str) -> dict:        
        """ runs three types of translations
            - decimal translation
            - datatypes with precision
            - datatypes with no precision
        """
        raw_dtype_cleanedup=self.__cleanup_raw_dtype(raw_dtype)
                
        if self.__is_decimaltype(raw_dtype = raw_dtype_cleanedup) :
           return self.__get_spark_decimal_type(raw_dtype_cleanedup) 
        return self.__parse_dtype(raw_dtype=raw_dtype_cleanedup)
        