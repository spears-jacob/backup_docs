-- set up some tables
drop table if exists test.an_specu_visits_per_device;
-- visits per device
create table test.an_specu_visits_per_device as
    SELECT
        `date`,
        CONCAT(YEAR(`date`), '-', lpad(MONTH(`date`), 2, 0)) AS year_month,
        visit.device.uuid as devices,
        CASE
            WHEN visit.device.operating_system like 'Android%' or
               visit.device.operating_system like '2.3%' then 'Android'
            WHEN visit.device.operating_system like 'iOS%' or visit.device.operating_system like 'iPhone%' or
               visit.device.operating_system like 'OS%' then 'iOS'
            ELSE visit.device.operating_system
        END as OS,
        visit.account.details['university'] as university,
        count(distinct(visit.visit_id)) as visits
    FROM
        prod.stva_events
    WHERE
        `date` >= '2016-09-01'
    GROUP BY
        `date`,
        CONCAT(YEAR(`date`), '-', lpad(MONTH(`date`), 2, 0)),
        visit.device.uuid,
        CASE
            WHEN visit.device.operating_system like 'Android%' or
               visit.device.operating_system like '2.3%' then 'Android'
            WHEN visit.device.operating_system like 'iOS%' or visit.device.operating_system like 'iPhone%' or
               visit.device.operating_system like 'OS%' then 'iOS'
            ELSE visit.device.operating_system
        END,
        visit.account.details['university']

select * from test.an_specu_visits_per_device  limit 1000

-- devices by # of visits across whole date range
--      sum visits before doing distinct counts
-- only for 4 unis; total across all
select
    a.num_visits,
    -- university,
    count(distinct(a.device_id))
FROM
(
    SELECT
        devices as device_id,
        -- university,
        sum(visits) as num_visits
    from
        test.an_specu_visits_per_device
    WHERE
        `date` BETWEEN '2016-09-01' AND '2016-12-15'
        -- year_month != '2016-12'
        -- and university != 'STVA'
        and university in ('Minnesota State University at Mankato', 'St. Louis University', 'University of Wisconsin', 'UW La Crosse')
    GROUP BY
        devices--, university
) a
group BY
    a.num_visits
    -- university


-- devices by # of visits across whole date range
--      sum visits before doing distinct counts
-- only for 4 unis; show each
select
    a.num_visits,
    university,
    count(distinct(a.device_id))
FROM
(
    SELECT
        devices as device_id,
        university,
        sum(visits) as num_visits
    from
        test.an_specu_visits_per_device
    WHERE
        `date` BETWEEN '2016-09-01' AND '2016-12-15'
        -- year_month != '2016-12'
        -- and university != 'STVA'
        and university in ('Minnesota State University at Mankato', 'St. Louis University', 'University of Wisconsin', 'UW La Crosse')
    GROUP BY
        devices, university
) a
group BY
    a.num_visits,
    university
