USE ${env:DASP_db};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.resultset.use.unique.column.names=false;

-- should deal with this using metadata repair.....
-- MSCK REPAIR TABLE ${env:ENVIRONMENT}.atom_cs_call_care_data_3;

CREATE TABLE ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP}(
  metric       string,
  visit_type   string ,
  value        float ,
  fiscal_month string )
;

INSERT INTO ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP}
SELECT
 'cir' as metric,
 'Overall' as visit_type,
 sum(calls_with_visit)/sum(authenticated_visits) as value,
 '${hiveconf:FISCAL_MONTH}'
FROM (
                SELECT call_date,
			    sum(authenticated_visits) as authenticated_visits,
			    sum(calls_with_visit) as calls_with_visit
				FROM cs_call_in_rate
				WHERE call_date >='${hiveconf:subquery_start}'
                    AND call_date <=DATE_ADD('${hiveconf:subquery_start}',41)
				    AND customer_type<>'UNMAPPED'
            AND acct_agent_mso IN('TWC','BHN','CHR')
				    GROUP BY call_date
				) vd
                JOIN chtr_fiscal_month m on vd.call_date = m.partition_date
                WHERE m.fiscal_month = '${hiveconf:FISCAL_MONTH}'
                GROUP BY fiscal_month
;
INSERT INTO ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP}
SELECT
 'cir' as metric,
 'Overall - '||customer_type as visit_type,
 sum(calls_with_visit)/sum(authenticated_visits) as value,
 '${hiveconf:FISCAL_MONTH}'
FROM (
                SELECT call_date,
				customer_type,
			    sum(authenticated_visits) as authenticated_visits,
			    sum(calls_with_visit) as calls_with_visit
				FROM cs_call_in_rate
				WHERE call_date >='${hiveconf:subquery_start}'
                    AND call_date <=DATE_ADD('${hiveconf:subquery_start}',41)
				    AND customer_type in ('COMMERCIAL', 'RESIDENTIAL')
            AND acct_agent_mso IN('TWC','BHN','CHR')
				    GROUP BY call_date, customer_type
				) vd
                JOIN chtr_fiscal_month m on vd.call_date = m.partition_date
                WHERE m.fiscal_month = '${hiveconf:FISCAL_MONTH}'
                GROUP BY fiscal_month, 'Overall - '||customer_type
;
INSERT INTO ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP}
SELECT
 'cir' as metric,
 visit_type,
 sum(calls_with_visit)/sum(authenticated_visits) as value,
 '${hiveconf:FISCAL_MONTH}'
FROM (
                SELECT call_date,
				visit_type,
			    sum(authenticated_visits) as authenticated_visits,
			    sum(calls_with_visit) as calls_with_visit
				FROM cs_call_in_rate
				WHERE call_date >='${hiveconf:subquery_start}'
                    AND call_date <=DATE_ADD('${hiveconf:subquery_start}',41)
            AND acct_agent_mso IN('TWC','BHN','CHR')
				    GROUP BY call_date, visit_type
				) vd
                JOIN chtr_fiscal_month m on vd.call_date = m.partition_date
                WHERE m.fiscal_month = '${hiveconf:FISCAL_MONTH}'
                GROUP BY fiscal_month, visit_type
;
INSERT INTO ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP}
SELECT
 'dfcr' as metric,
 'Overall' as visit_type,
 sum(calls_with_visit)/sum(validated_calls) as value,
 '${hiveconf:FISCAL_MONTH}'
            FROM (
				SELECT c.customer_type,
				c.call_end_date_utc,
				m.fiscal_month,
				count(distinct (case when c.encrypted_padded_account_number_256!='+yzQ1eS5iRmWnflCmvNlSg==' AND c.enhanced_account_number = 0 then c.call_inbound_key end)) as validated_calls
				FROM ${env:ENVIRONMENT}.atom_cs_call_care_data_3 c
				JOIN chtr_fiscal_month m ON c.call_end_date_utc = m.partition_date
				WHERE call_end_date_utc >='${hiveconf:subquery_start}'
                    AND call_end_date_utc <=DATE_ADD('${hiveconf:subquery_start}',41)
				    AND m.fiscal_month = '${hiveconf:FISCAL_MONTH}'
				    AND segment_handled_flag=1
				GROUP BY c.customer_type, m.fiscal_month, c.call_end_date_utc
				) cd
                JOIN (
				SELECT call_date,
				visit_type,
				customer_type,
				sum(authenticated_visits) as authenticated_visits,
				sum(calls_with_visit) as calls_with_visit
				FROM cs_call_in_rate
				WHERE call_date >='${hiveconf:subquery_start}'
                    AND call_date <=DATE_ADD('${hiveconf:subquery_start}',41)
				    AND customer_type<>'UNMAPPED'
            AND acct_agent_mso IN('TWC','BHN','CHR')
				    GROUP BY call_date, visit_type, customer_type
				    ) vd on vd.call_date = cd.call_end_date_utc AND vd.customer_type = cd.customer_type
                GROUP BY fiscal_month
;

-- final insert and housekeeping
INSERT OVERWRITE TABLE ${env:DASP_db}.cs_prod_monthly_fiscal_month_metrics PARTITION (fiscal_month)
SELECT * FROM ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.cs_pm_fm_m_${env:CLUSTER}_${env:STEP};
