--https://jira.charter.com/browse/XGANALYTIC-7406
--1st: dev.dp1_visitors_cpni
--Make visitor id tbl (based on 'cpni-roadblock' being in the url)
create table IF NOT EXISTS dev.dp1_visitors_cpni AS

  SELECT DISTINCT(visit__device__uuid) as visitor_id 
  FROM prod.net_events
  WHERE partition_date BETWEEN '2017-05-01' AND '2017-07-25'
  AND LOWER(state__view__current_page__page_id) LIKE '%cpni-roadblock%'

--2nd: dev.dp2_message__name_cpni
--Make visitor id tbl (based on unique messages relating to 'reset' or 'call forwarding')
create table IF NOT EXISTS dev.dp2_message__name_cpni AS

  SELECT DISTINCT(visit__device__uuid) as visitor_id 
  FROM prod.net_events
  WHERE partition_date BETWEEN '2017-05-01' AND '2017-07-25'   
    AND 
      (lower(message__name) LIKE '%reset-final%' 
      OR lower(message__name) LIKE '%call forwarding save confirm%')

--3rd: dev.dp3_visitors_cpni_combined
--Make combined visitor tbl (based on prior tbls)
create table IF NOT EXISTS dev.dp3_visitors_cpni_combined AS

  SELECT DISTINCT(a.visitor_id) as visitor_id_final 
  FROM 
    dev.dp1_visitors_cpni a,
    dev.dp2_message__name_cpni b 

  WHERE a.visitor_id = b.visitor_id
  
--4th: dev.dp4_cpni_visitor_events
--Qry net events against combined tbls 
CREATE TABLE IF NOT EXISTS dev.dp4_cpni_visitor_events as
     
SELECT 
    visit__visit_id,  
    visit__referrer_link,
    visit__device__uuid as visit__device__uuid,
    visit__isp__ip_address,
    visit__account__account_number,
    visit__user__id,
    visit__location__country_name,
    visit__location__region,
    visit__location__city, 
    visit__location__state, 
    visit__location__zip_code, 
    message__category,
    message__name,
    message__sequence_number,
    state__view__current_page__name,
    state__view__current_page__page_id,
    partition_date,
    message__timestamp

FROM 
  prod.net_events a, 
  dev.dp3_visitors_cpni_combined b

WHERE 
  partition_date BETWEEN '2017-05-01' AND '2017-07-25'
AND 
  visit__device__uuid = b.visitor_id_final
  
--5th: dev.dp5_cpni_ip_accts_accessed
--SHOW IP's and nbr of accts accessed by mnth
CREATE TABLE IF NOT EXISTS dev.dp5_cpni_ip_accts_accessed as

SELECT
  visit__isp__ip_address,
  visit__location__country_name,
  visit__location__region,
  visit__location__city, 
  visit__location__state, 
  visit__location__zip_code,
  MONTH(partition_date) as month_date, 
  SIZE (COLLECT_SET(visit__account__account_number)) as nbr_accts_accessed
  
FROM dev.dp4_cpni_visitor_events
WHERE visit__account__account_number IS NOT NULL

GROUP BY
  visit__isp__ip_address,
  visit__location__country_name,
  visit__location__region,
  visit__location__city, 
  visit__location__state, 
  visit__location__zip_code,
  MONTH(partition_date) 

--6th: dev.dp6_cpni_ip
--SHOW IP's for visitors accessing more than 2 accts
CREATE TABLE IF NOT EXISTS dev.dp6_cpni_ip as

SELECT
  visit__isp__ip_address,
  visit__location__country_name,
  visit__location__region,
  visit__location__city, 
  visit__location__state, 
  visit__location__zip_code,
  month_date, 
  nbr_accts_accessed
  
FROM dev.dp5_cpni_ip_accts_accessed
WHERE nbr_accts_accessed > 2

--Nbr Accts Per IP regardless of month
--Final qry may not be the final answer, but is another way of looking at it
--it helps answer the question "for the entire range of time, which IP's 
--transacted on behalf of more than 2 accounts?"

SELECT
  visit__isp__ip_address,
  visit__location__country_name,
  visit__location__region,
  visit__location__city, 
  visit__location__state, 
  visit__location__zip_code,
  SUM(nbr_accts_accessed) as nbr_accts_accessed

FROM dev.dp5_cpni_ip_accts_accessed
GROUP BY 
  visit__isp__ip_address,
  visit__location__country_name,
  visit__location__region,
  visit__location__city, 
  visit__location__state, 
  visit__location__zip_code

   