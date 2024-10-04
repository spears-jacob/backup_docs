--SELECT *
--FROM prod.cs_calls_with_prior_visits
--WHERE
--call_date='2019-11-24'
--AND customer_type='RESIDENTIAL'
--AND agent_mso='TWC'
--AND account_agent_mso='TWC'
--AND visit_type='specnet'
--AND issue_description='Billing Inquiries'
--AND cause_description='Discount Ending'
--AND resolution_description ='Customer Education - New Rates'
--;

SEt visits_table=387455165365/prod_dasp.cs_calls_with_prior_visits;
set TEST_DATE=2021-03-10;

select
      customer_type
      ,agent_mso
      ,account_agent_mso
      ,visit_type
      ,issue_description
      ,cause_description
      ,resolution_description
      ,count(1) as segments
      ,count(distinct call_inbound_key) as calls
      ,count(distinct account_number) as accounts
      ,call_date
from `${hiveconf:visits_table}`
WHERE call_date='${hiveconf:TEST_DATE}'
  AND customer_type='RESIDENTIAL'
  AND agent_mso='TWC'
  AND account_agent_mso='TWC'
  AND visit_type='specnet'
  AND issue_description='Billing Inquiries'
  AND cause_description='Discount Ending'
  AND resolution_description ='Customer Education - New Rates'
group by
      customer_type
      ,agent_mso
      ,account_agent_mso
      ,visit_type
      ,issue_description
      ,cause_description
      ,resolution_description
      ,call_date
;
