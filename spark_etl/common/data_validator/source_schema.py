from typing import Tuple
from pyspark.sql.types import StructType,StructField
from sandbox.spark_etl.common.data_validator.dtypes_mapper_abstract import dtypes_mapper_abstract  
from sandbox.spark_etl.common.data_validator.dtype_mapper_hive import  dtypes_mapper_hive  


class source_schema(object):
    def __init__(self,list_of_dict : list,dtype_translator : dtypes_mapper_abstract):
        self.__metadata_list_of_dict = list_of_dict
        self.__dtypes_translator = dtype_translator
        self.__element_format = {
            "column_name" : str(),
            "required" : bool (),
            "data_type" : str (),
            "format" : str()
        }
        
    @staticmethod
    def __element_template_for_validation(input_element :dict) -> dict:
        return { key:type(value) for key,value in input_element.items() }
     
    def is_valid_list(self,input_list : list) -> Tuple: #(bool,list of dictionaries)
        template_element_format = source_schema.__element_template_for_validation(input_element=self.__element_format)
        
        invalid_elements = []
        for column_metadata_element in input_list:
            column_metadata_element_format = source_schema.__element_template_for_validation(input_element=column_metadata_element)
            
            if not template_element_format == column_metadata_element_format :
               invalid_elements.append(column_metadata_element)
        
        return len(invalid_elements) == 0 , invalid_elements
    
    def __add_extended_column_MD(self,field_dict : dict) -> dict:
         """translate the source datatype and adding resulting dictionary to the enhanced metadata
         MD has the following format
         
         {"spark_dtype":<spark_datatype>,"source_dtype" :<raw_dtype>,"precision" :<precision>,"scale":<scale>}
         
         scale attribute is defined only for decimal datatypes
         """
         column_MD_dict = self.__dtypes_translator.translate_dtype_to_sparkdtype(field_dict["data_type"])
         field_dict.update(column_MD_dict)
         return field_dict
    
    def __add_extended_MD_to_fieldMD_list(self) -> list:
         return [self.__add_extended_column_MD(column_MD) for column_MD in self.__metadata_list_of_dict]
        
    def get_struct_schema_with_sparkdtypes(self) -> StructType :
        schema = StructType([StructField(name=fieldMD["column_name"]
                              ,dataType=fieldMD["spark_dtype"]
                              ,nullable=not fieldMD["required"]
                              ,metadata = {"source_datatype": fieldMD["data_type"]
                                           ,"datatype_precision":fieldMD["precision"]
                                           ,"spark_datatype":fieldMD["spark_dtype"].simpleString()
                                           ,"format" : fieldMD["format"]
                                           ,"datatype_scale" : fieldMD.get("scale",None)
                                          }  
                              ) for fieldMD in self.__add_extended_MD_to_fieldMD_list()
                             ])
        return schema
        
        
# print(source_schema([]).is_valid_list([{"column_name":"col_name","required":True,"data_type":"integer","format":""}])            )
# print(source_schema([]).is_valid_list([{"column_name":"col_name","required":True,"data_typ":"integer","format":""}])            )

# raw_schema = [{
#             "column_name" : "COLUMN_1",
#             "required" : True,
#             "data_type" : "varchar(10)",
#             "format" : ""
#         },
#         {
#             "column_name" : "COLUMN_2",
#             "required" : False,
#             "data_type" : "decimal(10,4)",
#             "format" : ""
#         },
#         {
#             "column_name" : "COLUMN_2",
#             "required" : False,
#             "data_type" : "timestamp",
#             "format" : ""
#         }
        
# ]

# print(source_schema(list_of_dict = raw_schema,dtype_translator=dtypes_mapper_hive()))
