# Importing necessary libraries and connecting to civis platform
import pandas as pd
import numpy as np
import civis
client = civis.APIClient()
db = "UN High Commissioner for Refugees"

# Reading contacts demographics from civis
contacts_demographics = """
                            SELECT * FROM analytics.contacts_demographics
"""

dtype_dict = {'id': str, 'zip': str, 'age':str}   

# Reading the query 
contacts_demographics = civis.io.read_civis_sql(contacts_demographics,
                        database = db, use_pandas=True, dtype = dtype_dict)

# Create the dataset to be modified
contacts_demog_unknown_cal = contacts_demographics.copy()

# Changing all California residents demographic to UNKNOWN
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "age"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "race"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "subethnicity"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "gender"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "marital_status"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "parent"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "religion"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "income"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "household_net_worth_bucket"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "party"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "date_of_birth"] = 'UNKNOWN'
contacts_demog_unknown_cal.loc[contacts_demog_unknown_cal["state"] == "CA", "age_bucket"] = 'UNKNOWN'

# Adding the dataset to the Analytics schema
civis.io.dataframe_to_civis(contacts_demog_unknown_cal, database = db, table = 'analytics.contacts_demog_unknown_cal',
                             existing_table_rows='drop')