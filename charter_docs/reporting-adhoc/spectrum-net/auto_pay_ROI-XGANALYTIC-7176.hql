--SQL used for JIRA XG-7176
--Make table of unique accts and dates atp were setup from combined test table
CREATE TABLE dev.dp_ap_setup_by_date_acct_XG6463 as 
SELECT DISTINCT
  net_ap_otp_combined.account_number, 
  net_ap_otp_combined.ap_date_time 
  
FROM
test.net_ap_otp_combined
;

--Query to show nbr of accts that setup atp once, twice, three times, etc. (fee times a matey)
SELECT 
  COUNT(DISTINCT account_number) as unique_accts,
  atp_total.nbr_of_atp
from
    (  
      SELECT account_number, count(*) as nbr_of_atp
      FROM dev.dp_ap_setup_by_date_acct_XG6463
      GROUP BY account_number
    ) atp_total
Group by
  atp_total.nbr_of_atp;

--Pull Unique Acct, Min Atp Date, & Payment Group
SELECT a.account_number,
       MIN(a.min_atp_date) AS min_atp_date,
       b.payment_group
FROM (SELECT string(account_number) AS account_number,
             MIN(ap_date_time) AS min_atp_date
      FROM dev.dp_ap_setup_by_date_acct_XG6463
      WHERE account_number IS NOT NULL
      GROUP BY string(account_number)) a
  JOIN dev.dp_roi_final b ON a.account_number = b.acct_num
GROUP BY a.account_number, b.payment_group;
