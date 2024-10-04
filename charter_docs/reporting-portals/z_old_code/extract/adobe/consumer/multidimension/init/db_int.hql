USE ${env:ENVIRONMENT};

DROP view if exists  asp_v_adobe_consumer_ss_counts_hourly;

create view if not exists  asp_v_adobe_consumer_ss_counts_hourly as
select * from asp_metrics_detail_hourly
where (date_denver >= (DATE_SUB(CURRENT_DATE, 180))
  and domain = 'resi'
  AND metric IN ( 'ss_referring_domain',
                  'ss_referring_site_section',
                  'ss_support_articles_auth',
                  'ss_support_articles_unauth')
  AND source_table = 'asp_v_net_events');

CREATE TABLE IF NOT EXISTS asp_equipment_flow
 (message__category STRING,
  message__name STRING,
  domain STRING,
  instances bigint,
  visits bigint )
PARTITIONED BY
 (source_table STRING,
  date_denver STRING 
;

CREATE VIEW IF NOT EXISTS asp_v_equipment_flow AS
select * from prod.asp_equipment_flow;
