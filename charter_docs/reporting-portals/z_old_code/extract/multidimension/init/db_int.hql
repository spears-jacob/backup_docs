USE ${env:ENVIRONMENT};

DROP view if exists  asp_v_venona_msa_action_counts;

create view if not exists  asp_v_venona_msa_action_counts as
select * from prod.asp_metrics_detail
where (date_denver >= (DATE_SUB(CURRENT_DATE, 180))
  and domain = 'app'
  AND metric IN ('Page Views', 'Modal Views', 'Select Actions')
  AND source_table = 'asp_v_venona_events_portals_msa');

drop view if exists asp_v_venona_metric_detail;

CREATE VIEW if not exists asp_v_venona_metric_detail   AS
SELECT asp_v_metric_detail.detail
      ,asp_v_metric_detail.unit
      ,asp_v_metric_detail.metric
      ,asp_v_metric_detail.platform
      ,asp_v_metric_detail.value
      ,asp_v_metric_detail.date_denver
      ,asp_v_metric_detail.domain
      ,asp_v_metric_detail.source_table
      ,asp_v_metric_detail.company
      ,IF(SPLIT(asp_v_metric_detail.detail,'-')[0] is null, 'NULL', SPLIT(asp_v_metric_detail.detail,'-')[0]) AS category
      ,SPLIT(asp_v_metric_detail.detail,'-')[1] AS svc_call_category
      ,'Svc Call Failure'    as success_failure
      ,'http_response_code'  AS error_group
      ,SPLIT(asp_v_metric_detail.detail,'-')[3] AS error_code
      ,COUNT (1)             as nbr_rows
  FROM prod.asp_v_metric_detail
 WHERE ( asp_v_metric_detail.date_denver >= '2018-07-19' )
   and asp_v_metric_detail.source_table  = 'asp_v_venona_events_portals_msa'
   and asp_v_metric_detail.metric        <> 'api_call_failures'   --deprecated
  GROUP BY  asp_v_metric_detail.detail
           ,asp_v_metric_detail.unit
           ,asp_v_metric_detail.metric
           ,asp_v_metric_detail.platform
           ,asp_v_metric_detail.value
           ,asp_v_metric_detail.date_denver
           ,asp_v_metric_detail.domain
           ,asp_v_metric_detail.source_table
           ,asp_v_metric_detail.company
;

CREATE TABLE IF NOT EXISTS asp_bounces_entries
( page_name STRING,
  entries INT,
  bounces INT)
PARTITIONED BY
( domain STRING,
  date_denver STRING
)
;

CREATE VIEW IF NOT EXISTS asp_v_bounces_entries
as select * from prod.asp_bounces_entries;
