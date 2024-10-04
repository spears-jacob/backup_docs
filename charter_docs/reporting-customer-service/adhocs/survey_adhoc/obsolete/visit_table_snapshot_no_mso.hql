SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

DROP TABLE IF EXISTS dev_tmp.cs_adhoc_visits_last_month;
CREATE TABLE dev_tmp.cs_adhoc_visits_last_month
AS
SELECT 
        ev.visit__account__account_number as v_acct,     -- Customer account number encrypted with 128-bit key length
        ev.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
	lower(visit__application_details__application_name) as visit_type,
        min(CAST(ev.received__timestamp/1000 as BIGINT)) as visit_start
FROM prod.asp_v_venona_events_portals ev    
  INNER JOIN prod.quantum_atom_snapshot_accounts_v ac 
  on prod.aes_decrypt(ev.visit__account__account_billing_id) = prod.aes_decrypt256(ac.encrypted_legacy_account_number_256)
WHERE visit__account__account_number is not null                             -- Pull only authenticated visits
  AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
  AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
  AND visit__visit_id is not null
  AND lower(visit__application_details__application_name) in ('myspectrum','specnet')
  AND upper(ac.Customer_type)='RESIDENTIAL'
  AND partition_date_utc ='2019-04-25'
  AND partition_date_denver BETWEEN '2019-04-24' AND '2019-04-26'
--  AND partition_date_utc BETWEEN '2019-04-25' AND '2019-04-29'
GROUP BY ev.visit__visit_id, visit__account__account_number, lower(visit__application_details__application_name)
;

SELECT count(v_acct) FROM dev_tmp.cs_adhoc_visits_last_month limit 10;
