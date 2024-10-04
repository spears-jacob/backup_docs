SELECT YM AS Daily_Set_Agg_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference,
       NC AS Num_Days_with_Calls,
       NDID - NC AS Num_Days_with_data_missing_calls
FROM
  (SELECT month(date(label_date_denver)) AS m,
          year(date(label_date_denver)) AS y,
          SUBSTRING(label_date_denver, 1, 7) AS YM,
          COUNT (DISTINCT label_date_denver) AS NDID,
          COUNT (DISTINCT ddwc) AS NC
   FROM quantum_set_agg_portals
   LEFT JOIN (SELECT distinct label_date_denver as ddwc FROM quantum_set_agg_portals
              WHERE  grain = 'daily' AND cast(call_count_24h as int) > 0 AND unit_type= 'visits') ON label_date_denver = ddwc
   WHERE grain = 'daily'
   GROUP BY month(date(label_date_denver)),
            year(date(label_date_denver)),
            SUBSTRING(label_date_denver, 1, 7)
   ORDER BY YM) t
ORDER BY YM

SELECT label_date_denver as daily_date_missing_calls
FROM quantum_set_agg_portals
WHERE grain = 'daily'
AND   label_date_denver NOT IN
 (SELECT label_date_denver as ddwc FROM quantum_set_agg_portals
  WHERE  grain = 'daily' AND cast(call_count_24h as int) > 0 AND unit_type= 'visits')
GROUP BY label_date_denver
ORDER BY label_date_denver

-- Checks that daily metrics match between set agg and metric agg, displays mismatched days
select date(sa.label_date_denver) + interval '1' day as run_date, sa.label_date_denver, cast(sa.metric_value as bigint) as sa_value, ma.metric_value as ma_value, cast(sa.metric_value - ma.metric_value as bigint) as delta
from (select label_date_denver,
             sum(metric_value) as metric_value
      from quantum_set_agg_portals
      where application_name in ('specnet','smb','myspectrum','specmobile', 'spectrumcommunitysolutions')
      and grouping_id = 65503
      and grain = 'daily'
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
(select denver_date,
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
 from quantum_metric_agg_portals ma
 group by 1 ) as ma
on sa.label_date_denver = ma.denver_date
where cast(sa.metric_value - ma.metric_value as bigint) <> 0
order by label_date_denver
