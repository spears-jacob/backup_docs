USE ${env:ENVIRONMENT};

SELECT '\n\nNow running asp_app_agg_init...\n\n';


CREATE TABLE IF NOT EXISTS asp_app_monthly_agg
(   metric STRING,
    value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (company STRING, year_month_Denver STRING);

  CREATE TABLE IF NOT EXISTS asp_app_monthly_agg_calc
  (   company STRING,
      metric STRING,
      value DECIMAL(15,5),
      unit STRING,
      year_month_Denver STRING
  );

  CREATE TABLE IF NOT EXISTS asp_app_monthly_agg_my_bhn_raw
  (   company STRING,
      metric STRING,
      value BIGINT,
      unit STRING
  )
    PARTITIONED BY (year_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_monthly_agg_my_spc_raw
    (   company STRING,
        metric STRING,
        value BIGINT,
        unit STRING
    )
      PARTITIONED BY (year_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_monthly_agg_my_twc_raw
    (   company STRING,
        metric STRING,
        value BIGINT,
        unit STRING
    )
      PARTITIONED BY (year_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_monthly_app_figures
    (   metric STRING,
        value BIGINT    )
    PARTITIONED BY ( company STRING, year_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_monthly_app_figures_reviews
    (   company STRING,
        platform STRING,
        stars STRING,
        review STRING  )
    PARTITIONED BY (year_month_Denver STRING );


    CREATE TABLE IF NOT EXISTS asp_app_monthly_total_subscribers_bi
    (   metric STRING,
        value BIGINT    )
    PARTITIONED BY ( company STRING, year_month STRING );

    CREATE TABLE IF NOT EXISTS asp_app_monthly_adjustments
    (   unit STRING,
        value BIGINT,
        change_comment STRING,
        jira_ticket STRING    )
    PARTITIONED BY ( company STRING, year_month STRING, metric STRING );

    CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_app_monthly_adjustments_raw
    (   company STRING,
        year_month STRING,
        metric STRING,
        unit STRING,
        value BIGINT,
        change_comment STRING,
        jira_ticket STRING)
        row format delimited fields terminated by '\t'
        lines terminated by '\n'
        stored as textfile TBLPROPERTIES('serialization.null.format'='', 'auto.purge'='true');

------- fiscal monthly tables

CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_agg
(   metric STRING,
    value DECIMAL(15,5),
    unit STRING
)
  PARTITIONED BY (company STRING, year_fiscal_month_Denver STRING);

  CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_agg_calc
  (   company STRING,
      metric STRING,
      value DECIMAL(15,5),
      unit STRING,
      year_fiscal_month_Denver STRING
  );

  CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_agg_my_bhn_raw
  (   company STRING,
      metric STRING,
      value BIGINT,
      unit STRING
  )
    PARTITIONED BY (year_fiscal_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_agg_my_twc_raw
    (   company STRING,
        metric STRING,
        value BIGINT,
        unit STRING
    )
      PARTITIONED BY (year_fiscal_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_agg_my_spc_raw
    (   company STRING,
        metric STRING,
        value BIGINT,
        unit STRING
    )
      PARTITIONED BY (year_fiscal_month_Denver STRING );


    CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_app_figures
    (   metric STRING,
        value BIGINT    )
    PARTITIONED BY ( company STRING, year_fiscal_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_app_figures_reviews
    (   company STRING,
        platform STRING,
        stars STRING,
        review STRING  )
    PARTITIONED BY (year_fiscal_month_Denver STRING );

    CREATE TABLE IF NOT EXISTS asp_app_fiscal_monthly_trend_pre
    (unit STRING,
    value STRING,
    domain STRING,
    tableau_display STRING,
    numerator1 STRING,
    denominator1 STRING,
    numerator3 STRING,
    denominator3 STRING,
    numerator12 STRING,
    denominator12 STRING)
    PARTITIONED BY
    (company STRING,
    year_month STRING,
    metric STRING)
    ;

    CREATE TABLE IF NOT EXISTS app_fiscal_monthly_trend_perc
    (unit STRING,
    value STRING,
    mom_perc_chg DECIMAL (15,3),
    prior_3_mo_perc_chg DECIMAL (15,3),
    yoy_perc_chg DECIMAL (15,3),
    domain STRING,
    tableau_display STRING)
    PARTITIONED BY
    (company STRING,
    year_month STRING,
    metric STRING)
    ;


    -------------- Daily tables

    CREATE TABLE IF NOT EXISTS asp_app_daily_agg
    (   metric STRING,
        value DECIMAL(15,5),
        unit STRING
    )
      PARTITIONED BY (company STRING, date_Denver STRING);

      CREATE TABLE IF NOT EXISTS asp_app_daily_agg_calc
      (   company STRING,
          metric STRING,
          value DECIMAL(15,5),
          unit STRING,
          date_Denver STRING
      );

      CREATE TABLE IF NOT EXISTS asp_app_daily_agg_my_bhn_raw
      (   company STRING,
          metric STRING,
          value BIGINT,
          unit STRING
      )
        PARTITIONED BY (date_Denver STRING );

      CREATE TABLE IF NOT EXISTS asp_app_daily_agg_my_spc_raw
      (   company STRING,
          metric STRING,
          value BIGINT,
          unit STRING
      )
        PARTITIONED BY (date_Denver STRING );

      CREATE TABLE IF NOT EXISTS asp_app_daily_agg_my_twc_raw
      (   company STRING,
          metric STRING,
          value BIGINT,
          unit STRING
      )
        PARTITIONED BY (date_Denver STRING );

      CREATE TABLE IF NOT EXISTS asp_app_daily_app_figures
      (   metric STRING,
          value BIGINT    )
      PARTITIONED BY ( company STRING, date_Denver STRING );

      CREATE TABLE IF NOT EXISTS asp_app_daily_app_figures_reviews
      (   company STRING,
          platform STRING,
          stars STRING,
          review STRING  )
      PARTITIONED BY (date_Denver STRING );

-- VIEWS
        -- Make sure to add any new apps here
        CREATE VIEW IF NOT EXISTS asp_app_monthly_agg_raw_adj
        AS
          SELECT raw.company,
                 raw.metric,
                 COALESCE(adj.value,raw.value) as value,
                 raw.unit,
                 raw.year_month_Denver,
                 adj.change_comment,
                 adj.jira_ticket
          FROM (
                SELECt company,metric, SUM(value) as value,unit,year_month_Denver
                from( SELECt company,metric,value,unit,year_month_Denver
                      from asp_app_monthly_agg_my_bhn_raw
                        UNION ALL
                      SELECt company,metric,value,unit,year_month_Denver
                      from asp_app_monthly_agg_my_spc_raw
                        UNION ALL
                      SELECt company,metric,value,unit,year_month_Denver
                      from asp_app_monthly_agg_my_twc_raw
                    ) SeparateAdobeSuites
                GROUP BY company,metric,unit,year_month_Denver
                ) raw
          LEFT OUTER JOIN asp_app_monthly_adjustments adj
          ON  raw.company = adj.company
          AND raw.year_month_Denver = adj.year_month
          AND raw.metric = adj.metric ;

DROP VIEW IF EXISTS asp_v_fiscal_monthly_agg_app_amn;
CREATE VIEW IF NOT EXISTS asp_v_fiscal_monthly_agg_app_amn AS
SELECT  rep_suite AS domain,
        company,
        metric,
        value,
        unit,
        year_month,
        change_comment,
        NULL AS jira_ticket
FROM asp_v_amnesty
WHERE rep_suite='app'
  AND year_month < '2018-01'
      UNION ALL
SELECT 'app' AS domain,
        company,
        metric,
        SUM(value) AS value,
        unit,
        year_fiscal_month_Denver AS year_month,
        NULL AS change_comment,
        NULL AS jira_ticket
FROM( SELECT company,metric,value,unit,year_fiscal_month_Denver
        FROM asp_app_fiscal_monthly_agg_my_bhn_raw
          UNION ALL
        SELECT company,metric,value,unit,year_fiscal_month_Denver
        FROM asp_app_fiscal_monthly_agg_my_spc_raw
          UNION ALL
        SELECT company,metric,value,unit,year_fiscal_month_Denver
        FROM asp_app_fiscal_monthly_agg_my_twc_raw
     ) SeparateAdobeSuites
WHERE year_fiscal_month_denver >= '2018-01'
GROUP BY company,metric,unit,year_fiscal_month_Denver
;


CREATE VIEW IF NOT EXISTS asp_app_fiscal_monthly_agg_raw_adj
AS
SELECT raw.company,
       raw.metric,
       COALESCE(adj.value,raw.value) as value,
       raw.unit,
       raw.year_month AS year_fiscal_month_Denver,
       adj.change_comment,
       adj.jira_ticket
FROM asp_v_fiscal_monthly_agg_app_amn raw
LEFT OUTER JOIN asp_app_monthly_adjustments adj
  ON  raw.company = adj.company
  AND raw.year_month = adj.year_month
  AND raw.metric = adj.metric
  AND raw.domain = 'app'
;

CREATE VIEW IF NOT EXISTS asp_app_daily_agg_raw
AS
  SELECt  company,
          metric,
          value,
          unit,
          date_Denver
  from asp_app_daily_agg_my_bhn_raw
    UNION
  SELECt  company,
          metric,
          value,
          unit,
          date_Denver
  from asp_app_daily_agg_my_spc_raw
    UNION
  SELECt  company,
          metric,
          value,
          unit,
          date_Denver
  from asp_app_daily_agg_my_twc_raw  ;

CREATE VIEW IF NOT EXISTS asp_app_figures_downloads
AS
  SELECT *
  from app_figures_downloads ;

CREATE VIEW IF NOT EXISTS asp_app_figures_sentiment
  AS
    SELECT *
    from app_figures_sentiment ;
---------

create table IF NOT EXISTS asp_operational_daily
  (ReportDay STRING,
   metric STRING,
   value STRING,
   VsAVGPrior8wksSameDayOfWk DECIMAL(15,5),
   review_comment STRING,
   additional_comment STRING)
PARTITIONED BY (domain STRING, date_denver STRING);

DROP VIEW IF EXISTS asp_app_fiscal_monthly_trend_lags;
CREATE VIEW IF NOT EXISTS asp_app_fiscal_monthly_trend_lags AS
SELECT  (value-(LAG(value,1) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num1m,
        (LAG(value,1) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver)) AS den1m,
        (value-(LAG(value,3) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num3m,
        (LAG(value,3) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver)) AS den3m,
        (value-(LAG(value,12) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver))) AS num12m,
        (LAG(value,12) OVER (PARTITION BY company,metric ORDER BY year_fiscal_month_denver)) AS den12m,
        company,
        year_fiscal_month_denver,
        metric
FROM asp_app_fiscal_monthly_agg
;

--------------------------------------------------------------------------------
------------- ##### 'MySpectrum 6.0 Dashboard Views Below' ##### ---------------
--------------------------------------------------------------------------------


    --------------------------------------------------------------------------------
    ---------------------- ##### 'Existing Daily Calcs' ##### ----------------------
    --------------------------------------------------------------------------------

    DROP VIEW IF EXISTS asp_v_prepost6_calc4;
    CREATE VIEW IF NOT EXISTS asp_v_prepost6_calc4
    AS
    SELECT
    CAST(value AS DECIMAL(10,3)) AS value,
    CAST(NULL AS STRING) AS breakpoint,
    metric,
    unit,
    'asp' AS platform,
    'app' AS domain,
    company,
    date_denver,
    'asp_app_daily_agg' AS source_table
    FROM asp_app_daily_agg
    WHERE metric IN
      (
      '% Successful One Time Payment',
      '% Successful AutoPay Enrollment',
      '% Households Logged In',
      'MOM % Change Households Logged In',
      '3 Month % Change Households Logged In',
      '% Successful Username Recovery',
      '% Successful Password Recovery'
      )
    AND value IS NOT NULL
    And value > 0
    AND date_denver > '2017-12-31'
    ;

    DROP VIEW IF EXISTS asp_v_prepost6_calc5;
    CREATE VIEW IF NOT EXISTS asp_v_prepost6_calc5
    AS
    SELECT
    CAST(SUM(numerator) / SUM(denominator) AS DECIMAL(10,3)) AS value,
    CAST(NULL AS STRING) AS breakpoint,
    'One Time Payment Success Rate' AS metric,
    CAST(NULL AS STRING) AS unit,
    platform AS platform,
    domain AS domain,
    company AS company,
    date_denver AS date_denver,
    source_table AS source_table
    FROM
    (SELECT
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        CASE WHEN (num1.value IS NOT NULL
          AND num1.value <> '0'
          AND den1.value IS NOT NULL
          AND den1.value <> '0')
            THEN SUM(num1.value)
            ELSE NULL END AS numerator,
        CASE WHEN (den1.value IS NOT NULL
          AND den1.value <> '0'
          AND num1.value IS NOT NULL
          AND num1.value <> '0')
            THEN SUM(den1.value)
            ELSE NULL END AS denominator
        FROM
        asp_v_prepost6_daily pp1
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'numerator1' AS numerator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'one_time_payment_success.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) num1
            ON num1.company = pp1.company
            AND num1.date_denver = pp1.date_denver
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'denominator1' AS denominator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'one_time_payment_start.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) den1
            ON den1.company = pp1.company
            AND den1.date_denver = pp1.date_denver
        WHERE
        pp1.company <> 'Total Combined'
        GROUP BY
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        num1.value,
        den1.value,
        pp1.value
        ) totes
    GROUP BY
    date_denver,
    company,
    platform,
    domain,
    source_table
    ;

    --------------------------------------------------------------------------------
    ---------------------- ##### 'AutoPay Success Rate' ##### ----------------------
    -------------- ##### 'AutoPay Successes' / 'AutoPay Starts' ##### --------------
    --------------------------------------------------------------------------------

    DROP VIEW IF EXISTS asp_v_prepost6_calc6;
    CREATE VIEW IF NOT EXISTS asp_v_prepost6_calc6
    AS
    SELECT
    CAST(SUM(numerator) / SUM(denominator) AS DECIMAL(10,3)) AS value,
    CAST(NULL AS STRING) AS breakpoint,
    'AutoPay Success Rate' AS metric,
    CAST(NULL AS STRING) AS unit,
    platform AS platform,
    domain AS domain,
    company AS company,
    date_denver AS date_denver,
    source_table AS source_table
    FROM
    (SELECT
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        CASE WHEN (num1.value IS NOT NULL
          AND num1.value <> '0'
          AND den1.value IS NOT NULL
          AND den1.value <> '0')
            THEN SUM(num1.value)
            ELSE NULL END AS numerator,
        CASE WHEN (den1.value IS NOT NULL
          AND den1.value <> '0'
          AND num1.value IS NOT NULL
          AND num1.value <> '0')
            THEN SUM(den1.value)
            ELSE NULL END AS denominator
        FROM
        asp_v_prepost6_daily pp1
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'numerator1' AS numerator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'autopay_enroll_success.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) num1
            ON num1.company = pp1.company
            AND num1.date_denver = pp1.date_denver
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'denominator1' AS denominator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'autopay_enroll_start.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) den1
            ON den1.company = pp1.company
            AND den1.date_denver = pp1.date_denver
        WHERE
        pp1.company <> 'Total Combined'
        GROUP BY
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        num1.value,
        den1.value,
        pp1.value
        ) totes
    GROUP BY
    date_denver,
    company,
    platform,
    domain,
    source_table
    ;

    --------------------------------------------------------------------------------
    --------------- ##### 'Troubleshooting Success Rate Router' ##### --------------
    --- ### 'Troubleshooting Successes Router' / 'Modem/Router Reset Starts' ### --
    --------------------------------------------------------------------------------

    DROP VIEW IF EXISTS asp_v_prepost6_calc3;
    CREATE VIEW IF NOT EXISTS asp_v_prepost6_calc3
    AS
    SELECT
    CAST(SUM(numerator) / SUM(denominator) AS DECIMAL(10,3)) AS value,
    CAST(NULL AS STRING) AS breakpoint,
    'Troubleshooting Success Rate Router' AS metric,
    CAST(NULL AS STRING) AS unit,
    platform AS platform,
    domain AS domain,
    company AS company,
    date_denver AS date_denver,
    source_table AS source_table
    FROM
    (SELECT
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        CASE WHEN (num1.value IS NOT NULL
          AND num1.value <> '0'
          AND den1.value IS NOT NULL
          AND den1.value <> '0')
            THEN SUM(num1.value)
            ELSE NULL END AS numerator,
        CASE WHEN (den1.value IS NOT NULL
          AND den1.value <> '0'
          AND num1.value IS NOT NULL
          AND num1.value <> '0')
            THEN SUM(den1.value)
            ELSE NULL END AS denominator
        FROM
        asp_v_prepost6_daily pp1
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'numerator1' AS numerator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'router_reset_successes.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) num1
            ON num1.company = pp1.company
            AND num1.date_denver = pp1.date_denver
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'denominator1' AS denominator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'modem_router_reset_starts.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) den1
            ON den1.company = pp1.company
            AND den1.date_denver = pp1.date_denver
        WHERE
        pp1.company <> 'Total Combined'
        GROUP BY
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        num1.value,
        den1.value,
        pp1.value
        ) totes
    GROUP BY
    date_denver,
    company,
    platform,
    domain,
    source_table
    ;

    --------------------------------------------------------------------------------
    ---------------- ##### 'Troubleshooting Completion Rate' ##### -----------------
    ----- ### (All M/R Successes + Failures) / 'Modem Router Reset Starts' ### -----
    --------------------------------------------------------------------------------

    DROP VIEW IF EXISTS asp_v_prepost6_calc2;
    CREATE VIEW IF NOT EXISTS asp_v_prepost6_calc2
    AS
    SELECT
    CAST(SUM(numerator) / SUM(denominator) AS DECIMAL(10,3)) AS value,
    CAST(NULL AS STRING) AS breakpoint,
    'Internet Troubleshooting Success Rate' AS metric,
    CAST(NULL AS STRING) AS unit,
    platform AS platform,
    domain AS domain,
    company AS company,
    date_denver AS date_denver,
    source_table AS source_table
    FROM
    (SELECT
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        CASE WHEN (num1.value IS NOT NULL
          AND num1.value <> '0'
          AND den1.value IS NOT NULL
          AND den1.value <> '0')
            THEN SUM(num1.value)
            ELSE NULL END AS numerator,
        CASE WHEN (den1.value IS NOT NULL
          AND den1.value <> '0'
          AND num1.value IS NOT NULL
          AND num1.value <> '0')
            THEN SUM(den1.value)
            ELSE NULL END AS denominator
        FROM
        asp_v_prepost6_daily pp1
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'numerator1' AS numerator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'internet_equipment_reset_flow_successes.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) num1
            ON num1.company = pp1.company
            AND num1.date_denver = pp1.date_denver
          LEFT JOIN
              (SELECT
                platform AS platform,
                domain AS domain,
                company AS company,
                date_denver AS date_denver,
                source_table AS source_table,
                SUM(value) AS value,
                'denominator1' AS denominator1
                FROM
                asp_v_prepost6_daily
                WHERE
                metric RLIKE 'internet_equipment_reset_flow_starts.*'
                GROUP BY
                date_denver,
                company,
                platform,
                domain,
                source_table
              ) den1
            ON den1.company = pp1.company
            AND den1.date_denver = pp1.date_denver
        WHERE
        pp1.company <> 'Total Combined'
        GROUP BY
        pp1.company,
        pp1.date_denver,
        pp1.platform,
        pp1.domain,
        pp1.source_table,
        num1.value,
        den1.value,
        pp1.value
        ) totes
    GROUP BY
    date_denver,
    company,
    platform,
    domain,
    source_table
    ;

    --------------------------------------------------------------------------------
    --------- ### 'Combined MySpectrum 6.0 Pre/Post Daily View Union' ### ----------
    --------------------------------------------------------------------------------

    DROP VIEW IF EXISTS asp_v_prepost6_daily_unified;
    CREATE VIEW IF NOT EXISTS asp_v_prepost6_daily_unified
    AS
    SELECT
    value,
    breakpoint,
    metric,
    unit,
    platform,
    domain,
    company,
    date_denver,
    source_table
    FROM asp_v_prepost6_daily
    WHERE (date_denver >= '2018-01-01')
      UNION ALL
      SELECT
      value,
      breakpoint,
      metric,
      unit,
      platform,
      domain,
      company,
      date_denver,
      source_table
      FROM asp_v_prepost6_calc1
      WHERE (date_denver >= '2018-01-01')
        UNION ALL
        SELECT
        value,
        breakpoint,
        metric,
        unit,
        platform,
        domain,
        company,
        date_denver,
        source_table
        FROM asp_v_prepost6_calc2
        WHERE (date_denver >= '2018-01-01')
          UNION ALL
          SELECT
          value,
          breakpoint,
          metric,
          unit,
          platform,
          domain,
          company,
          date_denver,
          source_table
          FROM asp_v_prepost6_calc3
          WHERE (date_denver >= '2018-01-01')
            UNION ALL
            SELECT
            value,
            breakpoint,
            metric,
            unit,
            platform,
            domain,
            company,
            date_denver,
            source_table
            FROM asp_v_prepost6_calc4
            WHERE (date_denver >= '2018-01-01')
              UNION ALL
              SELECT
              value,
              breakpoint,
              metric,
              unit,
              platform,
              domain,
              company,
              date_denver,
              source_table
              FROM asp_v_prepost6_calc5
              WHERE (date_denver >= '2018-01-01')
                UNION ALL
                SELECT
                value,
                breakpoint,
                metric,
                unit,
                platform,
                domain,
                company,
                date_denver,
                source_table
                FROM asp_v_prepost6_calc6
                WHERE (date_denver >= '2018-01-01')
    ;

--------------------------------------------------------------------------------
---------------- ### Metric Detail (Failures) Grouped View ### -----------------
--------------------------------------------------------------------------------

DROP VIEW IF EXISTS asp_v_metric_detail_grouped;
CREATE VIEW IF NOT EXISTS asp_v_metric_detail_grouped AS
SELECT   detail
        ,unit
        ,metric
        ,platform
        ,value
        ,date_denver
        ,domain
        ,source_table
        ,company
        ,regexp_extract(detail, '\AMACTION:(.*)\Svc Call.*', 1) AS category
        ,CASE
          WHEN (REGEXP_REPLACE(regexp_replace (regexp_extract(detail, '\AMACTION:(.*)\Svc Call.*', 1), '([A-Z])', '_$1' ),' ','')) RLIKE '_.*'
          THEN SUBSTRING(REGEXP_REPLACE(regexp_replace (regexp_extract(detail, '\AMACTION:(.*)\Svc Call.*', 1), '([A-Z])', '_$1' ),' ',''),2)
          ELSE REGEXP_REPLACE(regexp_replace (regexp_extract(detail, '\AMACTION:(.*)\Svc Call.*', 1), '([A-Z])', '_$1' ),' ','')
        END AS svc_call_category
        ,CASE
          WHEN metric RLIKE '.*failures.*' THEN 'Svc Call Failure'
          WHEN metric RLIKE '.*successes.*' THEN 'Svc Call Success'
        END as success_failure
        ,CASE
          WHEN LOWER(regexp_extract(detail, '.*Failure-(.*)', 1)) RLIKE '.*gateway.*' THEN 'gateway error'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE 'An error.*' THEN 'error while sending request'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE 'Unauth.*' THEN 'unauth error'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE 'A task.*' THEN 'canceled task error'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE 'OK.*' THEN 'OK error'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE '.*Internal.*' THEN 'server error'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE 'Bad.*' THEN 'bad request error'
          WHEN regexp_extract(detail, '.*Failure-(.*)', 1) RLIKE '\(E|S0|S1).*' THEN 'error code'
          ELSE 'other'
        END as error_group
        ,regexp_extract(detail, '.*Failure-(.*)', 1) AS error_code
        ,COUNT (1) as nbr_rows
FROM asp_v_metric_detail
WHERE ( date_denver >= '2017-01-01' )
GROUP BY   detail
          ,unit
          ,metric
          ,platform
          ,value
          ,date_denver
          ,domain
          ,source_table
          ,company
;

DROP VIEW IF EXISTS asp_v_venona_metric_detail_grouped;
CREATE VIEW IF NOT EXISTS asp_v_venona_metric_detail_grouped AS
SELECT detail
      ,unit
      ,metric
      ,platform
      ,value
      ,date_denver
      ,domain
      ,source_table
      ,company
      ,IF(SPLIT(detail,'-')[0] is null, 'NULL', SPLIT(detail,'-')[0]) AS category
      ,SPLIT(detail,'-')[1] AS svc_call_category
      ,'Svc Call Failure' as success_failure
      ,'http_response_code' AS error_group
      ,SPLIT(detail,'-')[3] AS error_code
      ,COUNT (1) as nbr_rows
FROM asp_v_metric_detail
WHERE ( date_denver >= '2018-07-19' )
and metric = 'api_call_failures'
GROUP BY  detail
          ,unit
          ,metric
          ,platform
          ,value
          ,date_denver
          ,domain
          ,source_table
          ,company
;

DROP VIEW IF EXISTS asp_v_operational_daily;
create view IF NOT EXISTS asp_v_operational_daily as select * from prod.asp_operational_daily;
