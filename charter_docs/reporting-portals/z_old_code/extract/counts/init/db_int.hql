USE ${env:ENVIRONMENT};

DROP VIEW IF EXISTS asp_v_venona_events_portals;
create view if not exists asp_v_venona_events_portals as
select * from prod.venona_events_portals;

DROP VIEW IF EXISTS asp_v_venona_events_portals_msa;
create view if not exists asp_v_venona_events_portals_msa as
select * from prod.venona_events_portals
where visit__application_details__application_name = 'MySpectrum';

DROP VIEW IF EXISTS asp_v_venona_events_portals_specnet;
create view if not exists asp_v_venona_events_portals_specnet as
select * from prod.venona_events_portals
where LOWER(visit__application_details__application_name) = 'specnet';

DROP VIEW IF EXISTS asp_v_venona_events_portals_smb;
create view if not exists asp_v_venona_events_portals_smb as
select * from prod.venona_events_portals
where LOWER(visit__application_details__application_name) =  'smb';

DROP VIEW IF EXISTS asp_v_venona_events_specmobile;
create view if not exists asp_v_venona_events_specmobile as
select * from prod.venona_events_specmobile ;

DROP VIEW IF EXISTS asp_v_venona_staging_portals_msa;
create view if not exists asp_v_venona_staging_portals_msa as
select * from prod.venona_events_staging
where visit__application_details__application_name = 'MySpectrum';

DROP VIEW IF EXISTS asp_v_venona_staging_portals_specnet;
create view if not exists asp_v_venona_staging_portals_specnet as
select * from prod.venona_events_staging
where LOWER(visit__application_details__application_name) = 'specnet';

DROP VIEW IF EXISTS asp_v_venona_staging_portals_smb;
create view if not exists asp_v_venona_staging_portals_smb as
select * from prod.venona_events_staging
where LOWER(visit__application_details__application_name) =  'smb';

DROP VIEW IF EXISTS asp_v_venona_counts;
CREATE VIEW if not exists asp_v_venona_counts AS
  SELECT  value,
          metric,
          date_hour_denver,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table,
          "hourly"                  as grain
     FROM prod.asp_venona_counts_hourly
    WHERE (date_denver > DATE_SUB(CURRENT_DATE, 30))

UNION ALL
  SELECT  value,
          metric,
          "all hours"                   as date_hour_denver,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table,
          "daily"                       as grain
     FROM prod.asp_venona_counts_daily
    WHERE (date_denver > DATE_SUB(CURRENT_DATE, 180))
;

DROP VIEW IF EXISTS asp_v_all_counts;
create view asp_v_all_counts as
select * from asp_v_venona_counts;

CREATE TABLE if not exists asp_venona_counts_daily(
  value        bigint,
  metric       string COMMENT 'Metric Name')
COMMENT 'Daily aggregate table counts of instances, visits, and hhs across all consolidated metrics'
PARTITIONED BY (
  unit         string COMMENT 'Specifies item being counted (instances, visits, hhs, etc)',
  platform     string,
  domain       string,
  company      string,
  date_denver  string,
  source_table string COMMENT 'original table that metric is queried from')
  ;

CREATE TABLE if not exists asp_venona_counts_hourly(
  value            bigint,
  metric           string,
  date_hour_denver string)
PARTITIONED BY (
  unit             string,
  platform         string,
  domain           string,
  company          string,
  date_denver      string,
  source_table     string)
  ;

------- for our My Spectrum reporting that needs to also include legacy Adobe My TWC data
DROP VIEW IF EXISTS asp_v_msa_counts;

CREATE VIEW asp_v_msa_counts AS
  SELECT  value,
          metric,
          date_hour_denver,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table,
          'hourly' as grain
     FROM prod.asp_venona_counts_hourly
    WHERE (date_denver > DATE_SUB(CURRENT_DATE, 30)
       AND source_table IN ('asp_v_venona_events_portals_msa'))

UNION ALL

  SELECT  value,
          metric,
          'all hours' as date_hour_denver,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table,
          'daily' as grain
  FROM prod.asp_venona_counts_daily
    WHERE (date_denver > DATE_SUB(CURRENT_DATE, 180)
       AND source_table IN ('asp_v_venona_events_portals_msa'))

UNION ALL

SELECT value,
       metric,
       date_hour_denver,
       unit,
       platform,
       domain,
       company,
       date_denver,
       source_table,
       "hourly" as grain
FROM prod.asp_counts_hourly
WHERE (date_denver > DATE_SUB(CURRENT_DATE, 30)
   AND source_table IN ('asp_v_twc_app_events','asp_v_net_events'))

UNION

SELECT value,
       metric,
       "all hours" as date_hour_denver,
       unit,
       platform,
       domain,
       company,
       date_denver,
       source_table,
       "daily" as grain
 FROM prod.asp_counts_daily
 WHERE (date_denver > DATE_SUB(CURRENT_DATE, 180)
   AND source_table IN ('asp_v_twc_app_events','asp_v_net_events'))
;

DROP VIEW IF EXISTS asp_v_venona_counts_daily;
create view asp_v_venona_counts_daily as select * from prod.asp_venona_counts_daily;
