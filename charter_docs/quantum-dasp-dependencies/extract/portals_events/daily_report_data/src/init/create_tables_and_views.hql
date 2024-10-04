USE ${env:ENVIRONMENT};

create table if not exists asp_daily_report_data(
  reportday string,
  metric string,
  value string,
  vsavgprior8wkssamedayofwk decimal(15,5),
  review_comment string,
  additional_comment string,
  domain string
) PARTITIONED BY (
date_denver string
)
;

create table IF NOT EXISTS asp_daily_report_data_summary(
  reportday string,
  domain string,
  metric_type string,
  metric_count_threshold int,
  metric_count INT,
  status boolean
) PARTITIONED BY (
date_denver string
)
;

create view IF NOT EXISTS asp_v_daily_report_data as
select *
  from asp_daily_report_data;

create view IF NOT EXISTS asp_v_daily_report_data_summary as
select *
  from asp_daily_report_data_summary;
