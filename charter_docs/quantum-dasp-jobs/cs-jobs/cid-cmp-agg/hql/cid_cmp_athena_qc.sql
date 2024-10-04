-- examine for days missing entirely in extract
SELECT YM AS CID_CMP_Extract_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM (SELECT month(date(denver_date)) AS m,
          year(date(denver_date)) AS y,
          SUBSTRING(denver_date, 1, 7) AS YM,
          COUNT (DISTINCT denver_date) AS NDID
      FROM cs_cid_cmp_extract
      GROUP BY month(date(denver_date)),
            year(date(denver_date)),
            SUBSTRING(denver_date, 1, 7)
      ORDER BY YM) t
ORDER BY YM;

-- examine for days missing entirely in daily aggregate
SELECT YM AS CID_CMP_Agg_daily_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM (SELECT month(date(label_date_denver)) AS m,
          year(date(label_date_denver)) AS y,
          SUBSTRING(label_date_denver, 1, 7) AS YM,
          COUNT (DISTINCT label_date_denver) AS NDID
      FROM cs_cid_cmp_aggregate
      WHERE grain = 'daily'
      GROUP BY month(date(label_date_denver)),
            year(date(label_date_denver)),
            SUBSTRING(label_date_denver, 1, 7)
      ORDER BY YM) t
ORDER BY YM;

-- examine for days missing entirely in weekly aggregate
SELECT YM AS CID_CMP_Agg_weekly_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM (SELECT month(date(label_date_denver)) AS m,
          year(date(label_date_denver)) AS y,
          SUBSTRING(label_date_denver, 1, 7) AS YM,
          COUNT (DISTINCT label_date_denver) AS NDID
      FROM cs_cid_cmp_aggregate
      WHERE grain = 'weekly'
      GROUP BY month(date(label_date_denver)),
            year(date(label_date_denver)),
            SUBSTRING(label_date_denver, 1, 7)
      ORDER BY YM) t
ORDER BY YM;

-- examine for days missing entirely in fiscal_monthly aggregate
SELECT YM AS CID_CMP_Agg_fiscal_monthly_Year_Month,
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
   FROM cs_cid_cmp_extract
   GROUP BY month(date(denver_date)),
            year(date(denver_date)),
            SUBSTRING(denver_date, 1, 7)
   ORDER BY YM) t
LEFT JOIN
  (SELECT label_date_denver, count(label_date_denver) as ct_fm
   FROM cs_cid_cmp_aggregate
   WHERE grain='fiscal_monthly'
   GROUP BY label_date_denver) sa_fm ON YM = SUBSTRING(label_date_denver, 1, 7)
ORDER BY YM

-- examine for days missing entirely in monthly aggregate
SELECT YM AS CID_CMP_Agg_calendar_monthly_Year_Month,
       ct_m as M_count,
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
   FROM cs_cid_cmp_extract
   GROUP BY month(date(denver_date)),
            year(date(denver_date)),
            SUBSTRING(denver_date, 1, 7)
   ORDER BY YM) t
LEFT JOIN
  (SELECT label_date_denver, count(label_date_denver) as ct_m
   FROM cs_cid_cmp_aggregate
   WHERE grain='monthly'
   GROUP BY label_date_denver) sa_fm ON YM = SUBSTRING(label_date_denver, 1, 7)
ORDER BY YM