# Importing necessary libraries and connecting to civis platform
import pandas as pd
import numpy as np
from datetime import datetime
import civis
client = civis.APIClient()
db = "UN High Commissioner for Refugees"

# The query to select the necessary columns
query = """
    SELECT o.id,
           o.contactid,
           o.npsp__primary_contact__c, 
           s.npsp__contact__c,
           s.npsp__opportunity__c as op_id_soft,
           o.name, 
           s.npsp__contact_name__c, 
           s.npsp__role_name__c,
           s.npsp__amount__c,
           o.amount,
           o.stagename,
           o.probability,
           o.expectedrevenue,   
           o.type,
           o.isclosed,
           o.iswon,
           o.forecastcategory,
           o.forecastcategoryname,
           o.campaignid,
           o.ownerid,
           o.createdbyid,
           o.lastmodifieddate,
           o.lastmodifiedbyid,
           o.fiscalquarter,
           o.fiscalyear,
           o.fiscal, 
           o.hasopenactivity,
           o.hasoverduetask,
           o.npe01__contact_id_for_role__c,
           o.npe01__is_opp_from_individual__c,
           o.npe01__number_of_payments__c,
           o.npe01__payments_made__c,
           o.npo02__combinedrollupfieldset__c,
           o.npe03__recurring_donation__c,
           o.npsp__acknowledgment_date__c,
           o.npsp__acknowledgment_status__c,
           o.npsp__recurring_donation_installment_name__c,
           o.sfmc_primary_contact__c,
           o.umb_receipt_sent__c,
           o.caseopportunityid__c,
           o.pledge_donation_first_payment__c,
           o.letter_code__c,
           o.pledge_donation__c,
           o.engagement_plan_processed__c,
           o.remaining_balance__c,
           o.campaign_promotion_code__c,
           o.processed_date_time__c,
           o.roi_family_id__c,
           o.contactcaseid__c,
           o.is_offline__c,
           o.recurring_payment_order__c,
           o.anonymous_donation__c,
           o.donation_method__c,
           o.fund_allocation__c,
           o.fund_id__c,
           o.gl_code__c,
           o.origination_vendor__c,
           o.psp_code__c,
           o.roi_transaction_id__c,
           o.related_donation__c,
           o.revocable__c,
           o.source_code__c,
           o.source_contact_type__c,
           o.source_type__c,
           o.transaction_amount__c, 
           o.closedate,
           o.createddate
                      
    FROM ds_salesforce.opportunity as o
        LEFT JOIN ds_salesforce.npsp__partial_soft_credit__c as s
        ON o.id = s.npsp__opportunity__c
        
    ORDER BY o.closedate  
"""


dtype_dict = {'contactid':str, 'npsp__primary_contact__c':str, 'npsp__contact__c':str, 'npsp__contact_name__c':str,
              'id':str, 'npsp__role_name__c':str, 'op_id_soft':str, 'npe01__contact_id_for_role__c':str, 
              'npe03__recurring_donation__c':str, 'npsp__acknowledgment_date__c':str,
              'npsp__recurring_donation_installment_name__c':str, 'roi_family_id__c':str, 'contactcaseid__c':str, 
              'recurring_payment_order__c':str, 'gl_code__c':str, 'psp_code__c':str, 'related_donation__c':str}

# Read the data from civis
df = civis.io.read_civis_sql(query, database = db, dtype=dtype_dict, use_pandas=True)

# Drop duplicates
df = df.drop_duplicates()

# Create the new column whether or not npsp__primary_contact__c got changed 
df["changed_to_soft_credit"] = np.where((df["npsp__primary_contact__c"].isnull()) & 
                                        (df["npsp__role_name__c"] == "Donor Advised Fund") & 
                                        (df['npsp__contact__c'].notnull()), 'yes', 'no')

# Replace npsp__primary_contact__c variable with npsp__contact__c for Donor Advised Fund
df.loc[(df["npsp__role_name__c"] == "Donor Advised Fund") & (df['npsp__contact__c'].notnull()), 
           "npsp__primary_contact__c"] = df['npsp__contact__c']

# Replace contactid variable with npsp__contact__c for Donor Advised Fund
df.loc[(df["npsp__role_name__c"] == "Donor Advised Fund") & (df['npsp__contact__c'].notnull()), 
           "contactid"] = df['npsp__contact__c']

# Drop unnecessary columns
df = df.drop(['npsp__contact__c', 'npsp__contact_name__c', 'npsp__amount__c', 'op_id_soft'], axis = 1) 

# Save the table to the analytics schema
civis.io.dataframe_to_civis(df, database = db, table = 'analytics.opportunities_dafcontacts',
                             existing_table_rows='drop')
