set mapreduce.input.fileinputformat.split.maxsize=100000000;
set mapreduce.input.fileinputformat.split.minsize=100000000;

--Step 1: Get Quantum data for Spectrum.net, SB.net, and MySpectrum App for the new timeframe
DROP TABLE IF EXISTS dev_tmp.cs_page_visits_quantum_adhoc;
CREATE TABLE dev_tmp.cs_page_visits_quantum_adhoc
AS
SELECT distinct
        a.visit__account__account_number as account_number,     -- Customer account number encrypted with 128-bit key length
        a.visit__visit_id AS visit_id,                                          -- Visit ID is the unique identifier for each customer visit and carries through page views
        CASE WHEN b.customer__type is null then 'UNMAPPED'
          ELSE UPPER(b.customer__type) END as customer_type,
        a.message__category,
        a.message__name,
        CAST(a.received__timestamp/1000 as BIGINT) as received__timestamp,
        a.partition_date_utc,
        lower(visit__application_details__application_name) as visit_type                                            -- Create Visit_Type field to identify these visits as Spectrum.net
FROM prod.asp_v_venona_events_portals a    --TODO: should this be account_snapshot?                                               -- This table contains visits for Spectrum.net exclusively
  LEFT JOIN prod.account_current b on prod.aes_decrypt(a.visit__account__account_number) = prod.aes_decrypt256(b.account__number_aes256)
WHERE visit__account__account_number is not null                                -- Pull only authenticated visits
  AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
  AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
  AND visit__visit_id is not null
  AND partition_date_utc between '2019-04-01' AND '2019-04-02'--'2019-04-30'    
;

SELECT count(account_number) FROM dev_tmp.cs_page_visits_quantum_adhoc WHERE partition_date_utc>='2019-04-01';
