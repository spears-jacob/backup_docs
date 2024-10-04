create temporary table params_explosion as
select a.*
from epid
lateral view inline (params) a
;

select distinct name from params_explosion;

AccountNumber
AgentEmail
AuthorizationNumber
BillToMobile
BillToPhone
BusinessName
CustomerFirstName
CustomerLastName
CustomerPhoneNumber
EmailAddress
FTTP
GISColor
IBS
InstallationDate
InstallationDateandTime
InstallationPrice
InstallationSupportPhoneNumber
InstallationTime
InstallationType
MonthlyPrice
OrderDate
OrderDetailsHTML
OrderNumber
OrderSummary
OrderTotal
PaymentAmount
PaymentType
PhonePortinMissingAccountNumber1
PhonePortinMissingAccountNumber2
PhonePortinMissingAccountNumber3
PhonePortinMissingAccountNumber4
PhonePortinMissingAccountNumber5
PhonePortinMissingAccountNumber6
PhonePortinMissingAccountNumber7
PhonePortinMissingAccountNumber8
PhonePortinMissingAuthorizedUser1
PhonePortinMissingAuthorizedUser2
PhonePortinMissingAuthorizedUser3
PhonePortinMissingAuthorizedUser4
PhonePortinMissingAuthorizedUser5
PhonePortinMissingAuthorizedUser6
PhonePortinMissingAuthorizedUser7
PhonePortinMissingAuthorizedUser8
PhonePortinMissingInformation
PhonePortinMissingPIN1
PhonePortinMissingPIN2
PhonePortinMissingPIN3
PhonePortinMissingPIN4
PhonePortinMissingPIN5
PhonePortinMissingPIN6
PhonePortinMissingPIN7
PhonePortinMissingPIN8
PhonePortinMissingPhoneNumber1
PhonePortinMissingPhoneNumber2
PhonePortinMissingPhoneNumber3
PhonePortinMissingPhoneNumber4
PhonePortinMissingPhoneNumber5
PhonePortinMissingPhoneNumber6
PhonePortinMissingPhoneNumber7
PhonePortinMissingPhoneNumber8
PhonePortinMissingProvider1
PhonePortinMissingProvider2
PhonePortinMissingProvider3
PhonePortinMissingProvider4
PhonePortinMissingProvider5
PhonePortinMissingProvider6
PhonePortinMissingProvider7
PhonePortinMissingProvider8
SAID
SalesRepId
ServicesHTML
ShipToAddressLine1
ShipToAddressLine2
ShipToCity
ShipToPostalCode
ShipToState
ShipToStateProvinceCode
WorkOrderNumber
access_code
account_guid
account_number
account_number_last4
account_type
address
address_1
address_2
agreement_date
agreement_number
agreement_url
amount_financed
appt_date
appt_day_date
appt_end_datetime
appt_info
appt_qty
appt_start_datetime
appt_time_code
appt_window
bank_acct_number
bank_acct_type
bill_start_date
bill_stop_date
biller_code
biller_type_code
billing_start_date
business_name
call_back_number
call_first_number
cancel_base_url
cancel_xfer
cancellation_type
card_number
card_type
carrier_name
carrier_support_phone
cash_price
city
company_name
contacts
create_user_name_landing
current_date
cust_details
customer_name
customer_type
cv_bill_start_valid_days
delivery_date
delivery_time
down_payment
duration
email_heading
email_subject
email_text_1
email_text_2
enrolle_name
enrolled_date
enrollment_type
equip_qty
equipment
equipment_number
equipment_return_date
equipment_shipped
est_tax_fees
estimated_delivery_date
eta
firstName
first_name
from_address_1
from_address_2
from_city
from_state
from_zip
fttp_hookup_type
glympse_available
glympse_invite_url
hardcopy_stmt
hawaiiSale
high_fidelity
inviter_first_name
inviter_last_name
ivr_text_1
job_class
job_number
job_type
language_cd
lastName
last_four
last_name
like_for_like_codes
link
monthly_payment_amount
msa_flag
mso_source
name
new_mntly_charges
new_order
notification_date
notification_name
notification_trigger
number_of_installments
one_time_charges_total
order_details_hf
order_details_rt
order_nbr
order_number
order_reason_code
order_status
order_type
outage_eligible
owner_equip_cde
param1
param10
param2
param3
param4
param5
param6
param7
param8
param9
past_date_time
payment_amt
payment_date
payment_method
payment_mode
payment_schedule
phone_number
prev_recurring_charges_total
previous_mntly_charges
primary_first_name
property_name
prorated_charges_dt_period
prorated_charges_total
reference_id
reschedule_xfer
return_equipment_details
return_label_url
salesChannel
scheduled_delivery_date
security_code
sender_id
service_code_details
service_codes
shipDate
simple_variant
sms_text
solo_account_id
spanish_enabled
spc_division_id
specialDisclaimerMessage
state
sub_user_first_name
sub_user_last_name
sub_user_username
sundaySkyUniqueID
tech_arrival_time
tech_name
tech_pool
tenant_name
time_empty
time_zone_id
total_after_all_payment
total_charges
total_sale_price
tracking_number
tracking_url
transaction_date
user_name
value_number
verification_code_url
verify_code
videoEquipment
zip

##############################
# show all epid fields in Athena:
SELECT
accountnumber,
campaigncategory,
campaignconsumerid,
campaignversion,
companycd,
contactstrategy,
customertype,
eventdetail.attemptnum as eventdetail_attemptnum,
eventdetail.channel as eventdetail_channel,
eventdetail.commid as eventdetail_commid,
eventdetail.commmsgid as eventdetail_commmsgid,
eventdetail.contactid as eventdetail_contactid,
eventdetail.eventdatetime as eventdetail_eventdatetime,
eventdetail.eventtype as eventdetail_eventtype,
eventdetail.startdatetime as eventdetail_startdatetime,
eventdetail.templatedesc as eventdetail_templatedesc,
eventdetail.templateid as eventdetail_templateid,
eventdetail.templateversion as eventdetail_templateversion,
eventdetail.timezone as eventdetail_timezone,
eventpostid,
ordernumber,
params,
multimap_from_entries(params) as params_map,
serviceeventtype,
soloaccountid,
spcdivisionid,
tokenid,
transactionid,
triggersource,
partition_date
FROM prod_sec_repo_sspp.asp_sentmsgs_epid_raw
ORDER BY RAND() LIMIT 25

##############################
# show all header-params fields in Athena:
select
payload.accountnumber,
payload.businessgroupname,
payload.campaignconsumerid,
payload.comm.attemptnum,
payload.comm.callduration,
payload.comm.channel,
payload.comm.commid,
payload.comm.commreceiveddatetime,
payload.comm.commstatus,
payload.comm.communicationtype,
payload.comm.contactvalue,
payload.comm.contentlanguage,
payload.comm.customerresponse,
payload.comm.deliverystatus,
payload.comm.emaillink,
payload.comm.enddatetime,
payload.comm.errordescription,
payload.comm.extractedatcommlevel,
payload.comm.isspanishfirst,
payload.comm.jasid,
payload.comm.jcrid,
payload.comm.jobid,
payload.comm.jpdid,
payload.comm.startdatetime,
payload.comm.urlclicks,
payload.creator,
payload.customertype,
payload.jobdescription,
payload.jobparameters,
multimap_from_entries(payload.jobparameters) as jobparameters_map,
payload.sessionid,
payload.spcdivisionid,
payload.templateid,
payload.transactionid,
payload.userpreflanguage,
partition_date
FROM prod_sec_repo_sspp.asp_sentmsgs_hp_raw
ORDER BY RAND() LIMIT 25

############################
# finding templates:
SELECT distinct tokenid, eventdetail.templateid
FROM prod_sec_repo_sspp.asp_sentmsgs_epid_raw
where lower(tokenid) like '%proact%'
order by tokenid, eventdetail.templateid
#	tokenid	                      templateid
1	Proact_Maintenance_Intl_Resi	Proact_Maintenance_Intl_Resi_Email
2	Proact_Maintenance_Intl_Resi	Proact_Maintenance_Intl_Resi_SMS
3	Proact_Maintenance_Intl_Resi
4	Proact_Maintenance_Rmdr_Resi	Proact_Maintenance_Rmdr_Resi_Email
5	Proact_Maintenance_Rmdr_Resi	Proact_Maintenance_Rmdr_Resi_SMS
6	Proact_Maintenance_Rmdr_Resi


select distinct  payload.templateid
from asp_sentmsgs_hp_raw
where lower(payload.templateid) like '%proact%'
#	templateid
01	Proactive_Serv_Alert_Restoration_SMB_es-ES
02	Proactive_Serv_Alert_Outage_SMB_es-ES
03	Proactive_Serv_Alert_Restoration_Resi_es-ES
04	Proactive_Serv_Alert_Outage_Resi_es-ES
05	Proactive_Serv_Alert_Outage_Smb
06	Proactive_Serv_Alert_Outage_Resi
07	Proactive_Serv_Alert_Outage
08	Proactive_Serv_Alert_Restoration
09	Proactive_Serv_Alert_Restoration_Resi
10	Proactive_Serv_Alert_Restoration_Smb


############################
# getting results:
SELECT eventdetail.eventdatetime, spcdivisionid, accountnumber
FROM prod_sec_repo_sspp.asp_sentmsgs_epid_raw
where lower(tokenid) like '%proact%'
ORDER BY RAND() LIMIT 25

SELECT payload.comm.startdatetime, payload.spcdivisionid, payload.accountnumber
FROM prod_sec_repo_sspp.asp_sentmsgs_hp_raw
where lower(payload.templateid) like '%proact%'
ORDER BY RAND() LIMIT 25

############################
# finding lags:
# epid has 1 day lag
# 2022-10-02 partition date contains both 2022-10-02 and 2022-10-01 event_date
select count(pd), pd, evd
FROM (SELECT substr(eventdetail.eventdatetime,1,10) as evd, partition_date as pd FROM prod_sec_repo_sspp.asp_sentmsgs_epid_raw where lower(tokenid) like '%proact%') c
group by pd, evd
order by pd, evd

# hp has 2 month lag (62 days), tailing after 7days
select count(pd), pd, evd
FROM (SELECT substr(payload.comm.startdatetime,1,10) as evd, partition_date as pd FROM prod_sec_repo_sspp.asp_sentmsgs_hp_raw where lower(payload.templateid) like '%proact%') c
group by pd, evd
order by pd, evd

###############

