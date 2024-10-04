-- examine for days missing entirely
SELECT YM AS MSA_Adoption_final,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                           + interval '1' MONTH AS varchar), '%Y-%m-%d')) AS Num_Days_In_Month,
       NDID AS Num_Days_In_Data,
       date_diff('day', date_parse(CONCAT(cast(y AS varchar), '-', cast(m AS varchar), '-', '01'), '%Y-%m-%d'),
       date_parse(cast((date(CONCAT(cast(y AS varchar), '-', (cast(m AS varchar)), '-', '01')))
                          + interval '1' MONTH AS varchar), '%Y-%m-%d')) - NDID AS Difference
FROM
  (SELECT MONTH(date(label_date)) AS m,
          YEAR(date(label_date)) AS y,
          SUBSTRING(label_date, 1, 7) AS YM,
          COUNT (DISTINCT label_date) AS NDID
   FROM cs_msa_adoption_final
   GROUP BY MONTH(date(label_date)),
            YEAR(date(label_date)),
            SUBSTRING(label_date, 1, 7) )
ORDER BY YM;

-- list missing days to run (run_date = label_date in this case)
SELECT dtrs AS missing_dates
FROM (SELECT SUBSTR(CAST(dtr AS varchar),1,10) AS dtrs
     FROM (SELECT sequence(minld,maxld, interval '1' day) date_range
           FROM (SELECT MIN(DATE (label_date)) minld,
                        MAX(DATE (label_date)) maxld
                 FROM cs_msa_adoption_final))
      CROSS JOIN UNNEST(date_range) AS unn (dtr)
      )
WHERE dtrs NOT IN (select label_date from cs_msa_adoption_final)
ORDER BY dtrs;