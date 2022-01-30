from typing import Tuple


class source_schema(object):
    def __init__(self,list_of_dict : list):
        self.__list_of_dict = list_of_dict
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
        
        
# print(source_schema([]).is_valid_list([{"column_name":"col_name","required":True,"data_type":"integer","format":""}])            )
# print(source_schema([]).is_valid_list([{"column_name":"col_name","required":True,"data_typ":"integer","format":""}])            )