SELECT YM AS Medallia_Year_Month,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM
  (SELECT month(date(partition_date_utc)) AS m,
          year(date(partition_date_utc)) AS y,
          SUBSTRING(partition_date_utc, 1, 7) AS YM,
          COUNT (DISTINCT partition_date_utc) AS NDID
   FROM stg_dasp.asp_medallia_interceptsurvey
   GROUP BY month(date(partition_date_utc)),
        year(date(partition_date_utc)),
        SUBSTRING(partition_date_utc, 1, 7)
   ORDER BY YM) t
ORDER BY YM
;
