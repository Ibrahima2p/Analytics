DROP TABLE analytics.california_contacts;

CREATE TABLE analytics.california_contacts AS
SELECT * FROM ds_salesforce.contact 
WHERE UPPER(mailingstate) = 'CA' OR UPPER(mailingstate) = 'CALIFORNIA'
      OR 
      mailingpostalcode IN (SELECT zip 
                            FROM ds_referencefiles.state_city_zip 
                            WHERE state = 'CA');
