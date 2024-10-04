USE ${env:ENVIRONMENT};

SELECT 'Now running asp_app_agg_05...';

-- ALTER TABLE asp_app_monthly_agg DROP IF EXISTS PARTITION(year_month_Denver="${env:YM}") PURGE;
-- insert OVERWRITE calculations (which clears the PARTITION if needed)

SELECT '\n\nNow Inserting Calculations...\n\n';
INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_agg PARTITION(company, ${env:ymd})
SELECT metric, value, unit, company, ${env:ymd}
FROM asp_app_${env:CADENCE}_agg_calc
WHERE ( ("${env:IsReprocess}"=0 AND ${env:ymd}="${env:YM}")
          OR
        ("${env:IsReprocess}"=1
          AND SUBSTR(CONCAT(${env:ymd},'-01'),0,10) >= "${env:START_DATE}"
          AND SUBSTR(CONCAT(${env:ymd},'-01'),0,10) <  "${env:END_DATE}")
      );

-- insert App Figures downloads values
SELECT '\n\nNow Inserting App Figures...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg PARTITION(company, ${env:ymd})
SELECT metric, value, 'downloads', company, ${env:ymd}
FROM asp_app_${env:CADENCE}_app_figures
WHERE ( ("${env:IsReprocess}"=0 AND ${env:ymd}="${env:YM}")
          OR
        ("${env:IsReprocess}"=1
          AND SUBSTR(CONCAT(${env:ymd},'-01'),0,10) >= "${env:START_DATE}"
          AND SUBSTR(CONCAT(${env:ymd},'-01'),0,10) <  "${env:END_DATE}")
      );

-- insert aggregated data raw
SELECT '\n\nNow Inserting aggregated raw data...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg PARTITION(company, ${env:ymd})
SELECT metric, value, unit, company, ${env:ymd}
FROM ${env:adj}
WHERE ( ("${env:IsReprocess}"=0 AND ${env:ymd}="${env:YM}")
          OR
        ("${env:IsReprocess}"=1
          AND SUBSTR(CONCAT(${env:ymd},'-01'),0,10) >= "${env:START_DATE}"
          AND SUBSTR(CONCAT(${env:ymd},'-01'),0,10) <  "${env:END_DATE}")
      );
