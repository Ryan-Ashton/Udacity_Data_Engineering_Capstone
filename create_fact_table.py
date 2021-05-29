import quandl
import pandas as pd
import numpy as np
from datetime import datetime, date 


from table_preprocess import create_abs_meta_table, create_abs_data_table, create_market_cap_table, create_time_table

def create_fact_t(create_abs_data_table, create_market_cap_table, create_time_table):

  """
  Description: This function creates the final Fact Table with joins

  Arguments:
      create_abs_data_table: Uses the ABS Data Table for the final join
      create_market_cap_table: Uses the Market Cap Table for the final join
      create_time_table: Uses the Time Table for the final join

  Returns:
      Fact Table 
  """

  try:

    fact_t = pd.merge(create_abs_data_table, create_market_cap_table,
    how='left', left_on='ABS_Description', right_on='ABS_Description')

    fact_t = pd.merge(fact_t, create_time_table,
    how='left', left_on='date', right_on='date')

    fact_t = fact_t[fact_t["GIC ASX Definition"] != "NA"]

    fact_t["GIC ASX Definition"].replace(to_replace=np.nan, value="Definition Not Found - Check Map File", regex=False, inplace=True)



    fact_t.set_index("index", inplace=True)

    fact_t.to_csv(r"Output_Data\fact_table.csv", index=True)

  except:

    create_abs_data_table = pd.read_csv(r"Output_Data\abs_data.csv")
    create_market_cap_table = pd.read_csv(r"Output_Data\market_cap_table.csv")
    create_time_table = pd.read_csv(r"Output_Data\time_table.csv")

    fact_t = pd.merge(create_abs_data_table, create_market_cap_table,
    how='left', left_on='ABS_Description', right_on='ABS_Description')

    fact_t = pd.merge(fact_t, create_time_table,
    how='left', left_on='date', right_on='date')

    fact_t = fact_t[fact_t["GIC ASX Definition"] != "NA"]

    fact_t["GIC ASX Definition"].replace(to_replace=np.nan, value="Definition Not Found - Check Map File", regex=False, inplace=True)

    fact_t.set_index("index", inplace=True)

    fact_t.to_csv(r"Output_Data\fact_table.csv", index=True)

  return fact_t