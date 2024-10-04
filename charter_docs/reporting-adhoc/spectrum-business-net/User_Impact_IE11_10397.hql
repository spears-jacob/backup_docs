-- XGANALYTIC-10397
-- The below queries look to understand the potential impact of an Internet Explorer defect where account information is persisting across visits -- this is SMB only.
-- The queries are organized by number of accounts and usernames per Device ID and then later the number of accounts and usernames per Visit ID.
-- There are queries at the bottom that repeat all device id and visit id queries -- the only difference is that the where clause is adjusted to look at all browsers aside from Inernet Explorer and Edge.
-- Additionally, the bottom four queries use collect_set and the results were used to export a list of account numbers and usernames to excel

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------


SELECt " The start and end dates are used to set the time frame for the query in a single place.

Usage:          between ${hiveconf:start_date} AND ${hiveconf:end_date}
  instead of:   between           '2018-02-13' AND '2018-03-28'

" ;


SET start_date='2018-02-13' ;
SET end_date='2018-03-28' ;


SELECt " The browser list regular expression, using alternating ()

Usage:          lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})

  instead of:   (lower(visit__device__browser__name) rlike ('.*internet explorer 11.*')
                  OR lower(visit__device__browser__name) rlike ('.*internet explorer mobile 11.*')
                  OR lower(visit__device__browser__name) rlike ('.*edge.*')
                )
" ;

SET brower_list_regex='.*internet explorer 11|internet explorer mobile 11|edge.*' ;



SELECt " The browser NOT IN list,

Usage:         visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")

  instead of:  visit__device__browser__name
                  NOT IN ('Edge 14.14393',
                          'Edge 15.15063',
                          'Edge 15.15254',
                          'Internet Explorer 11.0',
                          'Internet Explorer Mobile 11.0',
                          'Microsoft Edge 12',
                          'Microsoft Edge 13',
                          'Microsoft Edge 14',
                          'Microsoft Edge 15',
                          'Microsoft Edge 16',
                          'Microsoft Edge 17',
                          'Microsoft Internet Explorer 11')
" ;

SET brower_not_in_list="'Edge 14.14393',
        'Edge 15.15063',
        'Edge 15.15254',
        'Internet Explorer 11.0',
        'Internet Explorer Mobile 11.0',
        'Microsoft Edge 12',
        'Microsoft Edge 13',
        'Microsoft Edge 14',
        'Microsoft Edge 15',
        'Microsoft Edge 16',
        'Microsoft Edge 17',
        'Microsoft Internet Explorer 11'
" ;

------------------------- DEVICE IDS---------------------------------
----- Device ids with distinct counts of account numbers and user ids
----- Devices with more than one user name by browser. Browsers are all IE or Edge.
--Q01_IE
select  a.visit__device__browser__name,
        count (distinct (visit__device__uuid)) as count_devices -- distinct number of devices with more than one user name on the device by browser
from
     (Select  visit__device__uuid,
              visit__device__browser__name,
              count(distinct(visit__user__id)) AS user_id -- distinct count of users on a shared device
      from asp_v_sbnet_events
      where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
      group by  visit__device__uuid,
                visit__device__browser__name
      having user_id > 1
    ) a
group by a.visit__device__browser__name
;


----- Devices with more than one account number
-- The total number of distinct devices that had more than one account number by browser. Browsers are all IE or Edge
--Q02_IE
select
a.visit__device__browser__name,
count (distinct (visit__device__uuid)) as count_devices -- distinct number of devices with more than one account number on the device by browser

from

    (
    Select
    visit__device__uuid,
    visit__device__browser__name,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number -- distinct account numbers by device id and browser

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})

    group by
    visit__device__uuid,
    visit__device__browser__name

    having dec_account_number > 1
    ) a

group by a.visit__device__browser__name
;

------- Devices with 1 user id but more than 1 account number by browser name. Browsers are all IE or Edge
--Q03_IE
select
a.visit__device__browser__name, -- distinct devices by browser name
count(distinct(a.visit__device__uuid)) as device_count -- distinct number of devices with one user name and more than one account number on the device by browser

from

    (
    Select
    visit__device__uuid,
    visit__device__browser__name,
    count(distinct(visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
    group by
    visit__device__uuid,
    visit__device__browser__name

    having user_id = 1 -- one user id
    AND dec_account_number > 1 -- more than one account number
    ) a

group by a.visit__device__browser__name
;

------- Devices with 1 acct number but more than 1 user id by browser name. Browsers are all IE or Edge
--Q04_IE
select
a.visit__device__browser__name, -- distinct devices by browser name
count(distinct (visit__device__uuid)) as device_count -- distinct number of devices with more than one user name and only one account number on the device by browser

from

    (
    Select
    visit__device__uuid,
    visit__device__browser__name,
    count(distinct(visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
    group by
    visit__device__uuid,
    visit__device__browser__name

    having user_id > 1 --  more than one user id
    AND dec_account_number = 1 -- one account number
    ) a

group by a.visit__device__browser__name
;

----- Distinct count of device ids, users, and account numbers with IE11, IE11 Mobile, or Microsoft Edge as a browser
Select
visit__device__browser__name,
count(distinct(visit__device__uuid)) AS count_devices,
count(distinct (visit__user__id)) AS user_id,
count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
group by
visit__device__browser__name
;

------------------------- VISIT IDS---------------------------------
----- Visit IDs with distinct counts of account numbers and user ids
-- Visits with more than 1 account number. Browsers are all IE or Edge
--Q05_IE
select
a.visit__device__browser__name, -- distinct number of visits by browser
Count(distinct(a.visit__visit_id)) as visit_count -- distinct number of visits with more than one account number by browser

from

    (
    Select
    visit__visit_id,
    visit__device__browser__name,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
    group by
    visit__visit_id,
    visit__device__browser__name

    having dec_account_number > 1 --more than one account number
    ) a

group by a.visit__device__browser__name
;


---- Visits with more than one user id. Browsers are all IE or Edge
--Q06_IE
select
a.visit__device__browser__name,
Count(distinct(a.visit__visit_id)) as visit_count -- distinct count of visits with more than one user ID by browser

from

    (
    Select
    visit__visit_id,
    visit__device__browser__name,
    count(distinct (visit__user__id)) AS user_id

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
    group by
    visit__visit_id,
    visit__device__browser__name

    having user_id > 1 -- more than one user id
    ) a

group by a.visit__device__browser__name
;

------- Visits with 1 user id but more than 1 account number. Browsers are all IE or Edge
--Q07_IE
select
a.visit__device__browser__name,
count(distinct (a.visit__visit_id)) as visit_count -- distinct count of visits by browser with one user id and multiple account numbers

from

    (
    Select
    visit__visit_id,
    visit__device__browser__name,
    count(distinct (visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
    group by
    visit__visit_id,
    visit__device__browser__name

    having user_id = 1 -- one user id
    AND dec_account_number > 1 -- more than one account number
    ) a

group by a.visit__device__browser__name
;

------- Visits with 1 acct number but more than 1 user id. Browsers are all IE or Edge
--Q08_IE
select
a.visit__device__browser__name,
count(distinct(visit__visit_id)) as visit_count -- distinct count of visits by browser with more than one user id and only one account numbers

from

    (
    Select
    visit__visit_id,
    visit__device__browser__name,
    count(distinct(visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
        AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
    group by
    visit__visit_id,
    visit__device__browser__name

    having user_id > 1 -- more than one user id
    AND dec_account_number = 1 --one account number
    ) a

group by a.visit__device__browser__name
;

----- Distinct count of visit ids, users, and accounts with IE11, IE11 Mobile, or Microsoft Edge as a browser
--Q9_IE
Select
visit__device__browser__name,
count(distinct(visit__visit_id)) AS count_visits,
count(distinct(visit__user__id)) AS user_id,
count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
    AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})
group by
visit__device__browser__name
;

------------------------- OTHER BROWSERS ---------------------------------
--------------------------------------------------------------------------
----- Devices with more than one user ID excludes IE
--Q01_OB
select
'other browsers' AS visit__device__browser__name,
count (distinct (visit__device__uuid)) as count_devices -- distinct count of devices with more than one user ID
from

    (
      Select
      visit__device__uuid,
      count(distinct(visit__user__id)) AS user_id

      from asp_v_sbnet_events
      where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null

      group by
      visit__device__uuid

      having user_id > 1 -- more than one user id per device
    ) a

;

----- Device IDs with more than 1 account number excludes IE
--Q02_OB
select
'other browsers' AS visit__device__browser__name,
count (distinct (a.visit__device__uuid)) as count_devices -- distinct count of devices

from

    (
    Select
    visit__device__uuid,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null

    group by
    visit__device__uuid,
    visit__device__browser__name

    having dec_account_number > 1 -- more than one account number per device
    ) a

;

------- Devices with 1 user id but more than 1 account number, excludes IE
--Q03_OB
select
'other browsers' AS visit__device__browser__name,
count(distinct(a.visit__device__uuid)) as count_devices -- distinct number of devices

from

    (
    Select
    visit__device__uuid,
    visit__device__browser__name,
    count(distinct(visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null
    group by
    visit__device__uuid,
    visit__device__browser__name

    having user_id = 1 -- one user id
    AND dec_account_number > 1 -- more than one account number
    ) a

;

------- Devices with 1 acct number but more than 1 user id excludes IE
--Q04_OB
select
'other browsers' AS visit__device__browser__name,
count(distinct (a.visit__device__uuid)) as device_count --distinct number of devices

from

    (
    Select
    visit__device__uuid,
    visit__device__browser__name,
    count(distinct(visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null
    group by
    visit__device__uuid,
    visit__device__browser__name

    having user_id > 1 -- more than one user id
    AND dec_account_number = 1 -- one account number
    ) a
;

--- VISITS
----- Visits with more than one account number, excludes IE
--Q05_OB
select
'other browsers' AS visit__device__browser__name,
Count(distinct(a.visit__visit_id)) as visit_count -- distinct number of visits

from

    (
    Select
    visit__visit_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null

    group by
    visit__visit_id

    having dec_account_number > 1 -- more than one account number per visit
    ) a
;


----- Visits with more than 1 user name, excludes IE
--Q06_OB
select
'other browsers' AS visit__device__browser__name,
Count(distinct(a.visit__visit_id)) as visit_count -- distinct number of visits

from

    (
    Select
    visit__visit_id,
    count(distinct (visit__user__id)) AS user_id

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null

    group by
    visit__visit_id

    having user_id > 1 -- more than one user id per visit
    ) a

;

------- Visits with 1 user id but more than 1 account number, excludes IE
--Q07_OB
select
'other browsers' AS visit__device__browser__name,
count(distinct (a.visit__visit_id)) as visit_count -- distinct visits

from

  (
  Select
  visit__visit_id,
  count(distinct (visit__user__id)) AS user_id,
  count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

  from asp_v_sbnet_events
  where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
    AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
    AND visit__device__browser__name is not null
  group by
  visit__visit_id

  having user_id = 1 -- one user id
  AND dec_account_number > 1 -- more than one account number
  ) a

;

------- Visits with 1 acct number but more than 1 user id, excludes IE
--Q08_OB
select
'other browsers' AS visit__device__browser__name,
count(distinct(visit__visit_id)) as visit_count -- distinct visits

from

    (
    Select
    visit__visit_id,
    count(distinct(visit__user__id)) AS user_id,
    count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

    from asp_v_sbnet_events
    where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
      AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
      AND visit__device__browser__name is not null

    group by
    visit__visit_id,
    visit__device__browser__name

    having user_id > 1 --more than one user id
    AND dec_account_number = 1 -- one account number
  ) a
;

--------- distinct count of device ids and account numbers for all browsers other than IE11, IE11 Mobile, or Microsoft Edge
--Q09_OB
Select
'other browsers' AS visit__device__browser__name,
count(distinct(visit__visit_id)) AS count_visits,
count(distinct(visit__device__uuid)) AS count_devices,
count(distinct(visit__user__id)) AS user_id,
count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
  AND visit__device__browser__name NOT IN ("${hiveconf:brower_not_in_list}")
  AND visit__device__browser__name is not null
  ;



---------------------------------------------------------------------------------------------
--------------------------------- EXPORTING LISTS TO EXCEL ----------------------------------
---------------------------------------------------------------------------------------------
-- the below queries use collect set to get a list of usernames and account numbers that were exported into excel

-------------- get the list of user ids when there is more than 1 user id to a device

Select
visit__device__uuid,
visit__device__browser__name,
collect_set(visit__user__id),
count(distinct (visit__user__id)) AS user_id

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
    AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})

group by
visit__device__uuid,
visit__device__browser__name

having user_id > 1
;

------- list of account numbers when there is more than one account associated with a device
Select
visit__device__uuid,
visit__device__browser__name,
collect_set(prod.aes_decrypt(visit__account__account_number)),
count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
    AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})

group by
visit__device__uuid,
visit__device__browser__name

having dec_account_number > 1
;

------- generates the list of account numbers when there is more than one account number to a visit

Select
visit__visit_id,
visit__device__browser__name,
collect_set(prod.aes_decrypt(visit__account__account_number)),
count(distinct(prod.aes_decrypt(visit__account__account_number))) as dec_account_number

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
    AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})

group by
visit__visit_id,
visit__device__browser__name

having dec_account_number > 1
;

--------- generates the list of user ids when there is more than one user id to a visit

Select
visit__visit_id,
visit__device__browser__name,
collect_set(visit__user__id),
count(distinct (visit__user__id)) AS user_id

from asp_v_sbnet_events
where (partition_date_utc between ${hiveconf:start_date} AND ${hiveconf:end_date})
    AND  lower(visit__device__browser__name) rlike (${hiveconf:brower_list_regex})

group by
visit__visit_id,
visit__device__browser__name

having user_id > 1
;
