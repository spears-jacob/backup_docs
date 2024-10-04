SELECT YM AS Fiscal_Monthly_Set_Agg_Year_Month,
       ct_fm as FM_count,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM
  (SELECT month(date(denver_date)) AS m,
          year(date(denver_date)) AS y,
          SUBSTRING(denver_date, 1, 7) AS YM,
          COUNT (DISTINCT denver_date) AS NDID
   FROM quantum_metric_agg_portals
   GROUP BY month(date(denver_date)),
            year(date(denver_date)),
            SUBSTRING(denver_date, 1, 7)
   ORDER BY YM) t
LEFT JOIN
  (SELECT label_date_denver, count(label_date_denver) as ct_fm
   FROM quantum_set_agg_portals
   WHERE grain='fiscal_monthly'
   GROUP BY label_date_denver) sa_fm ON YM = SUBSTRING(label_date_denver, 1, 7)
ORDER BY YM

-- Checks that fiscal_monthly metrics match between set agg and metric agg
select concat(substring(sa.fiscal_month,1,7),'-15') as run_date, sa.fiscal_month, cast(sa.metric_value as bigint) as sa_value, ma.metric_value as ma_value, sa.metric_value - ma.metric_value as delta
from (select label_date_denver as fiscal_month,
             sum(metric_value) as metric_value
      from "glue:arn:aws:glue:us-east-1:387455165365:catalog".prod_dasp.quantum_set_agg_portals
      where application_name in ('specnet','smb','myspectrum','specmobile', 'spectrumcommunitysolutions')
      and grouping_id = 65503
      and grain = 'fiscal_monthly'
      and unit_type = 'instances'
      and metric_name in ('portals_support_page_views',
                          'portals_one_time_payment_successes',
                          'portals_set_up_auto_payment_successes',
                          'portals_all_equipment_reset_flow_successes',
                          'portals_internet_equipment_reset_flow_successes',
                          'portals_tv_equipment_reset_flow_successes',
                          'portals_voice_equipment_reset_flow_successes',
                          'portals_view_online_statments',
                          'portals_equipment_confirm_edit_ssid_select_action',
                          'portals_scp_click_pause_device',
                          'portals_scp_click_unpause_device',
                          'portals_scp_click_cancel_pause_device',
                          'portals_site_unique')
    group by 1) as sa
left join
(select fiscal_month||'-28' as fiscal_month,
        sum(portals_support_page_views +
        portals_one_time_payment_successes +
        portals_set_up_auto_payment_successes +
        portals_all_equipment_reset_flow_successes +
        portals_internet_equipment_reset_flow_successes +
        portals_tv_equipment_reset_flow_successes +
        portals_voice_equipment_reset_flow_successes +
        portals_view_online_statments +
        portals_equipment_confirm_edit_ssid_select_action +
        portals_scp_click_pause_device +
        portals_scp_click_unpause_device +
        portals_scp_click_cancel_pause_device +
        portals_site_unique) as metric_value
 from "glue:arn:aws:glue:us-east-1:387455165365:catalog".prod_dasp.quantum_metric_agg_portals ma
 left join "glue:arn:aws:glue:us-east-1:387455165365:catalog".prod_dasp.chtr_fiscal_month fm
 on ma.denver_date = fm.partition_date
 group by 1) as ma
on sa.fiscal_month = ma.fiscal_month
order by fiscal_month
