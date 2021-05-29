import pandas as pd
import numpy as np
from datetime import datetime, date 

def any_missing_data(table):

    """
    Description: This function checks if the tables don't have any nulls

    Arguments:
        table: Needs a table to check its data

    Returns:
        None
    """    
    
    if table.isnull().sum().sum() == 0:
        return print("Table looks good to use")
    else:
        raise Exception("ValueError exception thrown: Table contains nulls")


def date_range_check(fact_table):

    """
    Description: This function checks if the fact table has all its appropriate date ranges

    Arguments:
        fact_table: Needs the fact table to check its data

    Returns:
        None
    """        
    
    arr = fact_table.ABS_Description.unique()
    
    start_date = []
    end_date = []

    for a in arr:
        fact_table["ABS_Description"] == a
        start_date.append(fact_table.date.min())
        end_date.append(fact_table.date.max())

    start_date_set = set(start_date)
    end_date_set = set(end_date)


    if len(start_date_set) != 1 & len(end_date_set) != 1:
        raise Exception("There is an error with the ABS date range - investigate")
    else:
        print("Date range is correct")