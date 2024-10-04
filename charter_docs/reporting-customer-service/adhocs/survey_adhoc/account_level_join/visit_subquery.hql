SELECT DISTINCT
 account_number as v_acct
FROM dev_tmp.cs_page_visits_quantum_adhoc
WHERE partition_date_utc BETWEEN '2019-04-04' AND '2019-04-02';
