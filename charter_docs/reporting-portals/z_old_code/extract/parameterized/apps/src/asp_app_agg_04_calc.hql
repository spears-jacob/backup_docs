USE ${env:ENVIRONMENT};

SELECT 'Now running asp_app_agg_04_calc...';

-- Percent Successful One Time Payment
SELECT '\n\nNow selecting Percent Successful One Time Payment...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT otpstart.company,
       '% Successful One Time Payment' as metric,
        RPAD(
              ROUND((otpsuccess.value / otpstart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       otpstart.${env:ymd} as ${env:ymd}
from   ${env:adj} otpstart
INNER JOIN
   (SELECT ${env:ymd},
           value,
           company
    from ${env:adj}
    WHERE metric = 'One Time Payment Success') otpsuccess
on  TRIM(otpstart.${env:ymd}) = TRIM(otpsuccess.${env:ymd})
AND otpstart.company = otpsuccess.company
WHERE metric = 'One Time Payment Start';

-- Percent Successful Automatic Payment
SELECT '\n\nNow selecting Percent Successful Automatic Payment...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT apstart.company,
       '% Successful AutoPay Enrollment' as metric,
        RPAD(
              ROUND((apsuccess.value / apstart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       apstart.${env:ymd} as ${env:ymd}
from   ${env:adj} apstart
INNER JOIN
   (SELECT ${env:ymd},
           value,
           company
    from ${env:adj}
    WHERE metric = 'AutoPay Enroll Success') apsuccess
on  TRIM(apstart.${env:ymd}) = TRIM(apsuccess.${env:ymd})
AND apstart.company = apsuccess.company
WHERE metric = 'AutoPay Enroll Start';

-- Percent Successful Username Change
SELECT '\n\nNow selecting Percent Successful Username Change...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT fustart.company,
       '% Successful Username Recovery' as metric,
        RPAD(
              ROUND((fusuccess.value / fustart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       fustart.${env:ymd} as ${env:ymd}
from   ${env:adj} fustart
INNER JOIN
   (SELECT ${env:ymd},
           value,
           company
    from ${env:adj}
    WHERE metric = 'Forgot Username Success') fusuccess
on TRIM(fustart.${env:ymd}) = TRIM(fusuccess.${env:ymd})
AND fustart.company = fusuccess.company
WHERE metric = 'Forgot Username Attempt';

-- Percent Successful Password Change
SELECT '\n\nNow selecting Percent Successful Password Change...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT fpstart.company,
       '% Successful Password Recovery' as metric,
        RPAD(
              ROUND((fpsuccess.value / fpstart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       fpstart.${env:ymd} as ${env:ymd}
from   ${env:adj} fpstart
INNER JOIN
   (SELECT ${env:ymd},
           value,
           company
    from ${env:adj}
    WHERE metric = 'Forgot Password Success') fpsuccess
on  TRIM(fpstart.${env:ymd}) = TRIM(fpsuccess.${env:ymd})
AND fpstart.company = fpsuccess.company
WHERE metric = 'Forgot Password Attempt';

-- Percent Households Logged In
SELECT '\n\nNow selecting Percent Households Logged In...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT visitors.company,
       '% Households Logged In' as metric,
        RPAD(
              ROUND((visitors.value / subscribers.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       ${env:ymd} as ${env:ymd}
from   ${env:adj} visitors
INNER JOIN
   (SELECT company,
           year_month,
           value
    from   asp_app_monthly_total_subscribers_bi
    WHERE  metric = 'Total Subscribers') as subscribers
on TRIM(visitors.${env:ymd}) = TRIM(subscribers.year_month)
AND visitors.company = subscribers.company
WHERE metric = 'Unique Visitors'
AND "${env:CADENCE}" <> 'daily';

-- Totals (BHN and TWCC currently -- from aggregated tables)
-- Will need to add any new apps here
SELECT '\n\nNow selecting Total Combined for every metric excluding app downloads...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT 'Total Combined' as company,
       metric,
       SUM(value),
       MAX(unit),
       ${env:ymd}
FROM ${env:adj}
GROUP BY metric, ${env:ymd}
ORDER BY metric, ${env:ymd};

-- Totals (BHN and TWCC currently -- from app figures table)
SELECT '\n\nNow selecting Total Combined for app downloads...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT 'Total Combined' as company,
       metric,
       sum(value) as combinedValue,
       'downloads' as unit,
       ${env:ymd}
FROM asp_app_${env:CADENCE}_app_figures
GROUP BY metric,
         ${env:ymd};

-- Total Combined percentage calculations (asp_app_${env:CADENCE}_agg_calc)
-- Percent Households Logged In for Total Combined
SELECT '\n\nNow selecting Percent Households Logged In for Total Combined...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT visitors.company,
       '% Households Logged In' as metric,
        RPAD(
              ROUND((visitors.value / subscribers.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       ${env:ymd} as ${env:ymd}
from   asp_app_${env:CADENCE}_agg_calc visitors
INNER JOIN
   (SELECT 'Total Combined' as company,
           year_month,
           sum(value) as value
    from   asp_app_monthly_total_subscribers_bi
    WHERE  metric = 'Total Subscribers'
    GROUP BY year_month) as subscribers
on visitors.${env:ymd} = subscribers.year_month
AND visitors.company = subscribers.company
WHERE metric = 'Unique Visitors'
AND "${env:CADENCE}" <> 'daily';

-- Percent Successful One Time Payment (Total Combined)
SELECT '\n\nNow selecting Percent Successful One Time Payment (Total Combined)...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT company,
       '% Successful One Time Payment' as metric,
        RPAD(
              ROUND((otpsuccess.value / otpstart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       otpstart.${env:ymd} as ${env:ymd}
from   asp_app_${env:CADENCE}_agg_calc otpstart
INNER JOIN
   (SELECT ${env:ymd},
          value
    from asp_app_${env:CADENCE}_agg_calc
    WHERE metric = 'One Time Payment Success') otpsuccess
on otpstart.${env:ymd} = otpsuccess.${env:ymd}
WHERE metric = 'One Time Payment Start';

-- Percent Successful Automatic Payment (Total Combined)
SELECT '\n\nNow selecting Percent Successful Automatic Payment (Total Combined)...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT company,
       '% Successful AutoPay Enrollment' as metric,
        RPAD(
              ROUND((apsuccess.value / apstart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       apstart.${env:ymd} as ${env:ymd}
from   asp_app_${env:CADENCE}_agg_calc apstart
INNER JOIN
   (SELECT ${env:ymd},
          value
    from asp_app_${env:CADENCE}_agg_calc
    WHERE metric = 'AutoPay Enroll Success') apsuccess
on apstart.${env:ymd} = apsuccess.${env:ymd}
WHERE metric = 'AutoPay Enroll Start';



-- ALL Companies
-- % chg from prior mo to current month
SELECT '\n\nNow selecting % chg from prior mo to current month...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT company,
       metric,
       ROUND(calcvalue,5),
       'percentage' as unit,
       ${env:ymd}
FROM (
  SELECT company,
         ${env:ymd},
         'MOM % Change Households Logged In' as metric,
         (value - (lag( value , 1) over (partition by company order by ${env:ymd} )))/ (lag( value , 1) over (partition by company order by ${env:ymd} )) as calcvalue
  FROM asp_app_${env:CADENCE}_agg_calc
  WHERE metric = '% Households Logged In') src
WHERE src.calcvalue IS NOT NULL
AND "${env:CADENCE}" <> 'daily';

-- % chg from 3 months ago to current month
SELECT '\n\nNow selecting % chg from 3 months to current month...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT company,
       metric,
       ROUND(calcvalue,5),
       'percentage' as unit,
       ${env:ymd}
FROM (
  SELECT company,
         ${env:ymd},
         '3 Month % Change Households Logged In' as metric,
         (value - (lag( value , 3) over (partition by company order by ${env:ymd} )))/ (lag( value , 3) over (partition by company order by ${env:ymd} ))   as calcvalue
  FROM asp_app_${env:CADENCE}_agg_calc
  WHERE metric = '% Households Logged In') src
WHERE src.calcvalue IS NOT NULL
AND "${env:CADENCE}" <> 'daily';

-- Percent Successful Username Change (Total Combined)
SELECT '\n\nNow selecting Percent Successful Username Change (Total Combined)...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT company,
       '% Successful Username Recovery' as metric,
        RPAD(
              ROUND((fusuccess.value / fustart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       fustart.${env:ymd} as ${env:ymd}
from   asp_app_${env:CADENCE}_agg_calc fustart
INNER JOIN
   (SELECT ${env:ymd},
          value
    from asp_app_${env:CADENCE}_agg_calc
    WHERE metric = 'Forgot Username Success') fusuccess
on fustart.${env:ymd} = fusuccess.${env:ymd}
WHERE metric = 'Forgot Username Attempt'
and fusuccess.value IS NOT NULL;

-- Percent Successful Password Change (Total Combined)
SELECT '\n\nNow selecting Percent Successful Password Change (Total Combined)...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT company,
       '% Successful Password Recovery' as metric,
        RPAD(
              ROUND((fpsuccess.value / fpstart.value),5)
            ,7,'0') as value,
       'percentage' as unit,
       fpstart.${env:ymd} as ${env:ymd}
from   asp_app_${env:CADENCE}_agg_calc fpstart
INNER JOIN
   (SELECT ${env:ymd},
          value
    from asp_app_${env:CADENCE}_agg_calc
    WHERE metric = 'Forgot Password Success') fpsuccess
on fpstart.${env:ymd} = fpsuccess.${env:ymd}
WHERE metric = 'Forgot Password Attempt'
and fpsuccess.value IS NOT NULL;

------------------------------------------------------------
-- add placeholders for null values of calculated percentages,
-- make sure to add additional raw tables when new apps are added
SELECT '\n\nNow selecting placeholders for null values of calculated percentages...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT DISTINCT all_cm.company,
                all_cm.metric,
                null as value,
                'derived null percentage' as unit,
                all_cm.${env:ymd}
FROM (SELECT DISTINCT
        co.company,
        metric,
        ${env:ymd}
      FROM asp_app_${env:CADENCE}_agg_calc amac
      CROSS JOIN (SELECT DISTINCT company
                  FROM asp_app_${env:CADENCE}_agg_calc
                  WHERE company <> 'Total Combined' ) co
      WHERE metric LIKE '%\%%' and metric NOT LIKE 'App%') all_cm
LEFT OUTER JOIN (SELECT company, metric, ${env:ymd} from asp_app_${env:CADENCE}_agg_calc) raw
ON all_cm.company = raw.company
AND all_cm.metric = raw.metric
AND all_cm.${env:ymd} = raw.${env:ymd}
WHERE raw.company is NULL;

------------------------------------------------------------
------------------------------------------------------------
-- adds placeholders for null values of raw counts
-- make sure to additional raw tables when new apps are added
SELECT '\n\nNow selecting placeholders for null values of raw values...\n\n';
INSERT INTO TABLE asp_app_${env:CADENCE}_agg_calc
SELECT DISTINCT all_cm.company,
                all_cm.metric,
                null as value,
                'derived null' as unit,
                all_cm.${env:ymd}
FROM (SELECT DISTINCT co.company, metric, ${env:ymd} FROM asp_app_${env:CADENCE}_agg_calc amac
      CROSS JOIN (SELECT DISTINCT company FROM asp_app_${env:CADENCE}_agg_calc WHERE company <> 'Total Combined' ) co
      WHERE metric NOT LIKE '%\%%') all_cm
LEFT OUTER JOIN (SELECT company, metric, ${env:ymd} from asp_app_${env:CADENCE}_agg_my_twc_raw
                 union
                 SELECT company, metric, ${env:ymd} from asp_app_${env:CADENCE}_agg_my_bhn_raw) raw
ON all_cm.company = raw.company
AND all_cm.metric = raw.metric
AND all_cm.${env:ymd} = raw.${env:ymd}
WHERE raw.company is NULL;
