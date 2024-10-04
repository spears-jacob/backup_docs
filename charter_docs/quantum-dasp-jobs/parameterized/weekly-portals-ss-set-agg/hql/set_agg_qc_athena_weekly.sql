SELECT YM AS Weekly_Set_Agg_Year_Month,
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
              WHERE  grain = 'weekly' AND cast(call_count_24h as int) > 0 AND unit_type= 'visits') ON label_date_denver = ddwc
   WHERE grain = 'weekly'
   GROUP BY month(date(label_date_denver)),
            year(date(label_date_denver)),
            SUBSTRING(label_date_denver, 1, 7)
   ORDER BY YM) t
ORDER BY YM
;


SELECT label_date_denver as weekly_date_missing_calls
FROM quantum_set_agg_portals
WHERE grain = 'weekly'
AND   label_date_denver NOT IN
 (SELECT label_date_denver as ddwc FROM quantum_set_agg_portals
  WHERE  grain = 'weekly' AND cast(call_count_24h as int) > 0 AND unit_type= 'visits')
GROUP BY label_date_denver
ORDER BY label_date_denver

-- Checks that weekly metrics match between set agg and metric agg
-- Superb windowing function example for Athena / Presto
select date(sa.label_date_denver) + interval '1' day as weekly_run_date, sa.label_date_denver, cast(sa.metric_value as bigint) as sa_value, ma.metric_value as ma_value, cast(sa.metric_value - ma.metric_value as bigint) as delta
from (select label_date_denver,
             sum(metric_value) as metric_value
      from quantum_set_agg_portals
      where application_name in ('specnet','smb','myspectrum','specmobile', 'spectrumcommunitysolutions')
      and grouping_id = 65503
      and grain = 'weekly'
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
    group by label_date_denver) as sa
left join (SELECT dd, sum(cast(daily_metric_sum as bigint))
                 OVER ( ORDER BY dd
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                      ) as metric_value
           FROM (select denver_date dd,
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
                     portals_site_unique) as daily_metric_sum
                from quantum_metric_agg_portals
                group by denver_date) dms
           ORDER BY dd) as ma
on (sa.label_date_denver = ma.dd)
where cast(sa.metric_value - ma.metric_value as bigint) <> 0
order by label_date_denver;
