from typing import List, Set, Dict, Tuple, Optional
from pyspark.sql import DataFrame, Column
from functools import reduce

class dataframe_utils(DataFrame):
    def __init__(self, df):
         super(self.__class__, self).__init__(df._jdf, df.sql_ctx)
         self._input_df = df
         
    def apply_col_funct_to_col_list(self,fnc : Column,col_list : List[str],col_prefix="_") -> dict :
        """apply a function to dataframe on all columns name from the list
        Args:
            fnc (object): function with input parameter column name and returning Column type
            col_list (list): List of columns of type Column
        Returns:
            dict: {"column_name_1": <function expression of Column Type1> .. "column_name_N": <function expression of Column TypeN> }
        """             
        assert all(map(lambda c : c in self._input_df.columns,col_list)), "some column names were not found in source dataframe columns {0} input columns {1}".format(self._input_df.columns,col_list)

        dict_of_funcApplied = { "{1}__{0}".format(col_prefix,key) : funct_expression for key , funct_expression in zip(col_list,map(fnc,col_list)) }
        
        return dict_of_funcApplied
        
        # columns_list = ['col1', 'col2', 'col3', 'col4']
        # reduce(lambda df, col: df.withColumn(col, lit('NULL')), columns_list, df).show()    