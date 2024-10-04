MSCK REPAIR TABLE `${env:MVNOTABLE}`;

WITH CTE_MVNO AS
(
  SELECT charter_ban_aes256
        , mobile_account_number_aes256
        ,substr(line_activation_date,0,10) as line_activation_date
        ,preacquisition_company
  FROM `${env:MVNOTABLE}`
  WHERE line_activation_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:activation_range_end}' -- because line_activation_date is a date time, BETWEEN works well
)
-- Below CTE pulls metric agg data for join for 31 days from date of interest
,CTE_metricAgg AS
(
  SELECT acct_id
        ,denver_date
        ,application_name
  FROM ${hiveconf:metric_table}
  WHERE denver_date BETWEEN '${hiveconf:start_date}' AND '${hiveconf:visit_range_end}' -- 31 day period from date of interest. Could be expressed with variables where DOI = date of interest: BETWEEN DOI AND DOI+31
  AND LOWER(application_name) IN ('specnet','myspectrum')
)

INSERT OVERWRITE TABLE ${hiveconf:output_table}
partition(line_activation_date)
SELECT  charter_ban_aes256
      ,mobile_account_number_aes256
      ,application_name
      ,MIN(metagg.denver_date) AS minVisitDate
      ,line_activation_date
FROM CTE_MVNO mv
LEFT JOIN CTE_metricAgg metagg ON mv.charter_ban_aes256 = metagg.acct_id
GROUP BY charter_ban_aes256, mobile_account_number_aes256, line_activation_date, application_name
LIMIT 10;

MSCK REPAIR TABLE ${hiveconf:output_table};

SELECT * FROM ${hiveconf:output_table} WHERE line_activation_date=${hiveconf:start_date} LIMIT 10;
