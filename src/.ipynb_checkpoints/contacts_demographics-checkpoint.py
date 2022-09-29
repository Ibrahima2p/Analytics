# Importing necessary libraries and connecting to civis platform
import pandas as pd
import numpy as np
from datetime import datetime
import civis
client = civis.APIClient()
db = "UN High Commissioner for Refugees"

# Query to select the necessary variables
contacts_demographics = """SELECT a.source_id,
                                  c.accountid,
                                  c.firstname,
                                  c.middlename,
                                  c.lastname,
                                  c.primary_address as street,
                                  c.secondary_address as street2,
                                  c.city,
                                  c.state,
                                  c.zip,
                                  c.county,
                                  b.coalesced_noncommercial_age,
                                  b.race5way_noncommercial,
                                  b.subethnicity_noncommercial,
                                  b.gender_noncommercial,
                                  b.marriage_noncommercial,
                                  b.parent_noncommercial,
                                  b.religion_noncommercial,
                                  b.household_income_bucket,
                                  b.household_net_worth_bucket,
                                  b.vf_reg_party,
                                  b.vb_dob
                                  
                             FROM staging.sf_donors_vb_id a
                                  LEFT JOIN ts.basic_noncommercial_client b 
                                      ON a.matched_id = b.voterbase_id
                                  LEFT JOIN staging.sf_donors_for_person_matching c 
                                      ON c.id = a.source_id
"""

dtype_dict = {'zip': str,  'vf_reg_party':str, 'race5way_noncommercial':str, 'subethnicity_noncommercial':str,
              'gender_noncommercial':str, 'marriage_noncommercial':str, 'parent_noncommercial':str, 'religion_noncommercial':str,
              'household_income_bucket':str, 'household_net_worth_bucket':str}  

# Reading the query from civis
contacts_demographics = civis.io.read_civis_sql(contacts_demographics, database = db, use_pandas=True,
                                                dtype = dtype_dict)

# Fuction to reorganize the variable party
def party(party):
    if party in ['Independent','Green','Other','Libertarian','Conservative','Working Fam']:
        return 'Others'
    if party in ['Unaffiliated','No Party']:
        return 'Unaffiliated'
    if party == 'Unknown':
        return 'UNKNOWN'
    if party is np.nan:
        return 'UNKNOWN'
    else:
        return party

# Function to clean and reoganize the variable race
def race(race):
    if race == 'AfAm':
        return 'African American'
    if race == 'Native':
        return 'Native American'
    if race is np.nan:
        return 'UNKNOWN'
    if race == 'Unknown':
        return 'UNKNOWN'
    else:
        return race

# Function to categorize age 
def age(age):
    if age < 18:
        return 'Less than 18'
    if (age>=18) & (age<=24):
        return '18-24'
    if (age>=25) & (age<=34):
        return '25-34'
    if (age>=35) & (age<=44):
        return '35-44'
    if (age>=45) & (age<=54):
        return '45-54'
    if (age>=55) & (age<=64):
        return '55-64'
    if (age>=65) & (age<=74):
        return '65-74'
    if (age>=75) & (age<=84):
        return '75-84'
    if age>=85:
        return 'Greater than 85'
    if age is np.nan:
        return 'UNKNOWN'
    else:
        return 'UNKNOWN'

def convert_date(date):
    return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

# Create the variable date of birth
contacts_demographics['vb_dob2'] = contacts_demographics['vb_dob'].fillna(18000101.0)
contacts_demographics['vb_dob2'] = contacts_demographics['vb_dob2'].astype(str)
contacts_demographics['vb_dob2'] = contacts_demographics['vb_dob2'].apply(lambda x: x.split('.')[0])
contacts_demographics['vb_dob2'] = contacts_demographics['vb_dob2'].apply(convert_date)
contacts_demographics['vb_dob2'] = np.where(contacts_demographics['vb_dob2']=='1800-01-01',np.nan,contacts_demographics['vb_dob2'])
contacts_demographics.drop('vb_dob',axis=1,inplace=True)

# Dictionary to map old names to new names
rename_dict = {'source_id':'contactid',
               'coalesced_noncommercial_age':'age',
               'race5way_noncommercial':'race',
               'subethnicity_noncommercial':'subethnicity',
               'gender_noncommercial':'gender',
               'marriage_noncommercial':'marital_status',
               'parent_noncommercial':'parent',
               'religion_noncommercial':'religion',
               'household_income_bucket':'income',
               'household_net_worth_bucket':'household_net_worth_bucket',
               'vf_reg_party':'party',
               'vb_dob2':'date_of_birth'}

# Renaming columns
contacts_demographics = contacts_demographics.rename(columns = rename_dict)

# Applying functions to make changes on the variables race and party, and create new variable age_bucket
contacts_demographics['race'] = contacts_demographics['race'].map(lambda x: race(x))
contacts_demographics['age_bucket'] = contacts_demographics['age'].map(lambda x: age(x))
contacts_demographics['party'] = contacts_demographics['party'].map(lambda x: party(x))

cols = ['gender','race','age_bucket','age','subethnicity','parent','income','party','marital_status',
          'religion','household_net_worth_bucket','date_of_birth']

# Replacing missing values with UNKNOWN
contacts_demographics[cols] = contacts_demographics[cols].fillna('UNKNOWN')

# Changing all California residents demographic to UNKNOWN
contacts_demographics.loc[contacts_demographics["state"] == "CA", "age"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "race"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "subethnicity"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "gender"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "marital_status"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "parent"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "religion"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "income"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "household_net_worth_bucket"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "party"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "date_of_birth"] = 'UNKNOWN'
contacts_demographics.loc[contacts_demographics["state"] == "CA", "age_bucket"] = 'UNKNOWN'

# Save the to the analytics schema in civis
civis.io.dataframe_to_civis(contacts_demographics, database = db, table = 'analytics.contacts_demographics',
                             existing_table_rows='drop')