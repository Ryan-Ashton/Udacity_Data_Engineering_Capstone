import quandl
import pandas as pd
import dask.dataframe as dd
import numpy as np
from datetime import datetime, date


def create_abs_meta_table(quandl_api_key, map_table, left='ABS_CODE_SEASONAL_ADJ', right='series_id'):

    """
    Description: This function creates a filtered metadata table by utilising a JSON mapping table

    Arguments:
        quandl_api_key: Needs the Quandl API Key to connect with Quandl
        map_table: Requires the mapping table to filter the metadata table

    Returns:
        Filtered ABS Metadate Table
    """    
    
    quandl.ApiConfig.api_key = quandl_api_key
    
    map_table = pd.read_json(map_table)

    map_table.set_index(left, inplace=True)
    
    meta_data = quandl.get_table("AUSBS/M", paginate=True)

    meta_data.set_index(right, inplace=True)
    
    abs_meta_data = pd.merge(map_table, meta_data, how='left', left_index=True, right_index=True)

    abs_meta_data.to_csv(r"Output_Data\abs_meta_data.csv", index=True)
    
    return abs_meta_data



def create_abs_data_table(data_location, abs_meta_data, left='series_id', right='series_id'):

    """
    Description: This function creates the ABS Data Table with its metadata

    Arguments:
        data_location: Needs the location of the CSV file
        abs_meta_data: Requires the previous table for the join

    Returns:
        Final ABS data table with metadata
    """        
    
    abs_data = dd.read_csv(data_location)
    
    abs_data = dd.merge(abs_meta_data, abs_data, how='left', left_index=True, right_on=right)
    
    abs_data["seasonal_adjusted_chain_volume_measure"] = abs_data["value"] * 1000000
    
    abs_data = abs_data.drop(['ABS_CODE_PERCENT_CHNG', 'series_id', 'type', 'units', 'month', 'value'], axis=1)

    abs_data = abs_data.compute()

    abs_data["index"] = abs_data["ABS_Description"].astype(str) + "-" + abs_data["date"].astype(str)

    abs_data.set_index("index", inplace=True)

    abs_data.to_csv(r"Output_Data\abs_data.csv", index=True)

    return abs_data



def create_market_cap_table(asx_data, map_table, left='GIC ASX Definition', right='GICs industry group'):

    """
    Description: This function creates the aggregated market cap table by industry

    Arguments:
        asx_data: Needs the location of the CSV file
        map_table: Requires the mapping table to join the relevant ABS data

    Returns:
        Final aggregated market cap table by industry
    """        
    
    asx_data = dd.read_csv(asx_data)
    
    map_table = pd.read_json(map_table)
    
    industry_asx_table = dd.merge(map_table, asx_data, how='left', left_on=left, right_on=right)
    
    industry_asx_table = industry_asx_table.compute()
    
    industry_asx_table = industry_asx_table.groupby(['ABS_Description']).agg({'Market Cap':['sum']})
    
    industry_asx_table.reset_index(inplace=True)
    
    industry_asx_table.columns = industry_asx_table.columns.droplevel(1)

    industry_asx_table.rename(columns={"Market Cap": "Market Cap Today"}, inplace=True)

    industry_asx_table.set_index("ABS_Description", inplace=True)

    industry_asx_table.to_csv(r"Output_Data\market_cap_table.csv", index=True)
    
    return industry_asx_table


def create_time_table(start='1/2/1950', end=date.today()):

    """
    Description: This function creates a time table to help expand the time intelligence

    Arguments:
        start: Gives the option to select the start date
        end: Gives the option to select the end date

    Returns:
        Final Time Table
    """        

    date_rng = pd.date_range(start=start, end=end, freq='D')

    time = pd.DataFrame(date_rng, columns=['date'])

    t = pd.to_datetime(time['date'], unit='ms')

    time['date'] = pd.to_datetime(time['date'], unit='ms')

    time_data = ([x, x.day, x.week, x.month, x.quarter, x.year] for x in t)

    column_labels = ('date', 'day', 'week', 'month', 'quarter', 'year')

    time_df = pd.DataFrame(time_data, columns=column_labels)

    time_df['date'] = time_df['date'].astype(str)

    time_df.set_index("date", inplace=True)

    time_df.to_csv(r"Output_Data\time_table.csv", index=True)

    return time_df