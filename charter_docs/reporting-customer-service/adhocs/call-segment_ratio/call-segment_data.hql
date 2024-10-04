SELECT
c.call_inbound_key
, p.visit__application_details__application_name
, count(DISTINCT c.segment_number)
FROM
prod.cs_call_data c
JOIN prod.asp_v_venona_events_portals p
ON prod.aes_decrypt(p.visit__account__account_number) = prod.aes_decrypt256(c.account_number) --Calls and visits are linked together based on customer account information
WHERE c.enhanced_account_number = 0
AND c.call_end_date_utc >= '2019-03-01'
AND c.call_end_date_utc<'2019-04-01'
GROUP BY call_inbound_key, p.visit__application_details__application_name
limit 10;

--lower(visit__application_details__application_name) as visit_type
