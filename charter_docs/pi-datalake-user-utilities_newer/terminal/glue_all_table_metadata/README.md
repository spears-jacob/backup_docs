## all_table_metadata.sh
#### Explore all_tables: name, updated when, stored where, and created by ...
_Explore all_tables for a point-in-time view of all available tables using the current credentials and region.  Table details are pulled down into several json files called ```<db_name>.json``` in a temporary folder that is then parsed by jq to pull out the relevant bits.  Then, grep can be run against the parsed file ```all_tables``` and there is a shortcut pointing to the file for later processing._

#### Requirements for running this script on a mac laptop.
- Make sure to have AWS credentials set beforehand
- Configure `~/.bash_profile` to allow use as needed from any terminal from the [common_utilities readme Usage Section #3](https://gitlab.spectrumflow.net/awspilot/pi-datalake-user-utilities/-/blob/master/terminal/common_utilities/README.md)

## all_table_metadata.sh Usage
```
source all_table_metadata.sh

  -- The results returned relate to the credentials currently exported

  -- So, make sure each of the following are available from the current terminal
   $AWS_ACCESS_KEY_ID
   $AWS_SECRET_ACCESS_KEY
   $AWS_SESSION_TOKEN
```

## Output resembles the following
| database.table_name| update_date_time| Storage_Descriptor_Location  | Created_by_arn_account_and_user_or_entity|
|--------------------|-----------------| ---------------------------- | -----------------------------------------|
|stg_dasp.hsd_usage| 2021-05-19T10:41:14-06:00| s3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/hsd_usage| arn:aws:sts::213705006773:assumed-role/chartersso-AP-PDM-AWS-SelfService-User/rward2|
|stg_dasp.idm_logins| 2021-02-25T15:41:10-07:00| s3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/idm_logins| arn:aws:sts::213705006773:assumed-role/chartersso-AP-PDM-AWS-SelfService-Admin/P2813160|
|stg_dasp.idm_logins_adhoc| 2021-09-14T14:36:57-06:00| s3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/idm_logins_adhoc| arn:aws:sts::213705006773:assumed-role/pi-qtm-dasp-stg-ssp-self-service-cluster-1-emr-ec2/i-01829e4ddb1515afb|
|stg_dasp.idm_page_views| 2021-02-25T15:43:28-07:00| s3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/idm_page_views| arn:aws:sts::213705006773:assumed-role/chartersso-AP-PDM-AWS-SelfService-Admin/P2813160|
|stg_dasp.idm_rpw_experience_adhoc| 2020-11-05T18:16:35-07:00| s3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/idm_rpw_experience_adhoc| arn:aws:sts::213705006773:assumed-role/dev-temp_cicd_worker_role/cicd-deploy-stg|
|stg_dasp.idm_specnet_logins| 2021-09-17T11:32:39-06:00| s3://pi-qtm-dasp-stg-aggregates-nopii/data/stg_dasp/idm_specnet_logins| arn:aws:sts::213705006773:assumed-role/pi-qtm-dasp-stg-ssp-self-service-cluster-1-emr-ec2/i-05949aef3825758fd|
|stg_dasp.invalid_emails| 2021-05-05T14:52:51-06:00| s3://pi-qtm-dasp-stg-aggregates-pii/data/stg_dasp/invalid_emails| arn:aws:sts::213705006773:assumed-role/pi-qtm-dasp-stg-ssp-self-service-cluster-1-emr-ec2/i-0092c554cd550f893|
