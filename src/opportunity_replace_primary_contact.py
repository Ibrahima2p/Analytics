# Importing necessary libraries and connecting to civis platform
import pandas as pd
import numpy as np
from datetime import datetime
import civis
client = civis.APIClient()
db = "UN High Commissioner for Refugees"

# The query to select the necessary columns
query = """
    SELECT o.npsp__primary_contact__c, 
           s.npsp__contact__c, 
           s.npsp__opportunity__c as op_id_soft, 
           o.id,
           o.name, 
           s.npsp__contact_name__c, 
           s.npsp__role_name__c,
           s.npsp__amount__c,
           o.amount,
           /* 
           o.stagename,
           o.probability,
           o.expectedrevenue,
           o.closedate,
           o.type,
           o.isclosed,
           o.iswon,
           o.forecastcategory,
           o.forecastcategoryname,
           o.campaignid,
           o.ownerid,
           o.creteddate,
           o.createdbyid,
           o.lastmodifieddate,
           o.lastmodifiedbyid,
           o.fiscalquarter,
           o.fiscalyear,
           o.fiscal,
           o.contactid,
           o.openactivity,
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
           o.npsp__primary_contact__c,
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
           */
           s.createddate
           
    FROM ds_salesforce.npsp__partial_soft_credit__c as s
        RIGHT JOIN ds_salesforce.opportunity as o
        ON s.npsp__opportunity__c = o.id
    INNER JOIN ds_salesforce.account AS a
        ON o.accountid = a.id
    WHERE o.closedate >= '2022-01-01' --AND s.createddate >= '2022-01-01'
            --s.npsp__role_name__c = 'Donor Advised Fund'     -- AND 
            --(a.membership_type__c = 'Major Donor' OR a.membership_type__c = 'Mid-Level')  
    ORDER BY o.contactid     
"""


dtype_dict = {'npsp__primary_contact__c':str, 'npsp__contact__c':str, 'npsp__contact_name__c':str, 
              'id':str, 'npsp__role_name__c':str}

# Read the data from civis
df = civis.io.read_civis_sql(query, database = db, dtype=dtype_dict, use_pandas=True)

# Create the new column whether or not a donation is soft credit
df.loc[df['op_id_soft'].notnull() , "soft_credit"] = 'yes'
df.loc[df['op_id_soft'].isnull() & df["npsp__primary_contact__c"].notnull(), "soft_credit"] = 'no'

# Replace npsp__primary_contact__c variable with npsp__contact__c for Donor Advised Fund
df.loc[(df["npsp__role_name__c"] == "Donor Advised Fund") & (df['npsp__contact__c'].notnull()), 
           "npsp__primary_contact__c"] = df['npsp__contact__c']

# Save the table to the analytics schema
civis.io.dataframe_to_civis(df, database = db, table = 'analytics.opportunity_replace_primary_contact.py',
                             existing_table_rows='drop')
