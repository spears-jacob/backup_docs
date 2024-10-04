The tableau onlineForm Errors Dashboard workbook with worksheet 'Error Count Dash' is refreshed after the job is done:
https://pi-datamart-tableau.corp.chartercom.com/#/views/OnlineFormErrorsDashboard/ErrorCountDash?:iid=2

The tableau onlineForm Errors Dashboard workbook with worksheet 'Call-In Rate Dash' is refreshed after the job is done:
https://pi-datamart-tableau.corp.chartercom.com/#/views/OnlineFormErrorsDashboard/Call-InRateDash?:iid=3


Summary:
Error Count Dashboard for onlineForms that shows inline error field and message information per Page Title (online form) & Call-In Rate Dashboard for onlineForms that shows Call-In Rate trend per Page Title and Submission Action Types Tableau Dash Tableau Link: https://pi-datamart-tableau.corp.chartercom.com/#/workbooks/6334/views
Description:
The online form error count dash has been an existing tableau dashboard used by stakeholder. It was adjusted due to the change of encryption in the errorMessage field of errorExtras at the end of December 2021. The call-in rate dashboard is new and alls for seeing call-in rate by each form individually and also by submission type (i.e. Failure No Success). Screenshots of the tableaus for both are included in https://jira.charter.com/browse/XGANALYTIC-33345 and the working link about with 2022 data that I have manually refreshed is above in this MR.
stg run link: https://us-east-1.console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-BZU7257JF66C
XG tickets related to this work:
https://jira.charter.com/browse/XGANALYTIC-31384
&
https://jira.charter.com/browse/XGANALYTIC-33345