--------- JIRA TICKET XGANALYTIC-10055
------ The design team wants to see the number of clicks on 4 different icons from the contact us page,
------ the photos of the icons are included in the Jira tag. We are not ingesting data for the order online icon
------ The ask is for the data to be aggregated at the monthly level for the last 2 months
------ The below query should generate the number of clicks per icon

SELECt " The start and end dates are used to set the time frame for the query in a single place.

Usage:          BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date}
  instead of:   between '2018-02-01' and '2018-03-31'

" ;

SET start_date='2018-02-01' ;
SET   end_date='2018-03-31' ;

select
-- each sum if statement looks to see if the visit started on the contact us page, and then if
-- within the next 3 message__names they clicked on an icon, the click is counted
-- we use lead here because there are other ways to get to the ask spectrum page,
-- or find store, etc. but we want the number of clicks that started from the contact us page with icons
SUM(IF(a.message__name = 'support.category.general.contact-us',1,0))  AS icon_page, -- this is the page where a user would access the icons -- all users must start here
SUM(IF(a.message__name = 'support.category.general.contact-us'
  AND (a.next_page = 'Ask-Spectrum' OR a.next_page2 = 'Ask-Spectrum'
  OR a.next_page3 = 'Ask-Spectrum'),1,0))  AS ask_spectrum_icon,
SUM(IF(a.message__name = 'support.category.general.contact-us'
  AND (a.next_page = 'Login.zip' OR a.next_page2 = 'Login.zip'
  OR a.next_page3 = 'Login.zip'),1,0))  AS phone_icon,
SUM(IF(a.message__name = 'support.category.general.contact-us'
  AND (a.next_page = 'support.locations'
  OR a.next_page2 = 'support.locations'
  OR a.next_page3 = 'support.locations'),1,0))  AS find_store_icon,
SUM(IF(a.message__name = 'support.category.general.contact-us'
  AND (a.next_page = 'com:buyflow:localization-resp'
    OR a.next_page2 = 'com:buyflow:localization-resp'
    OR a.next_page3 = 'com:buyflow:localization-resp'),1,0))  AS order_online_icon,
SUBSTRING(a.partition_date,1,7) AS year_month

FROM
  (select -- the below query should generate a visit id and the following 3 pages the user went to, ordered by message timestamp
          partition_date,
          message__timestamp,
          message__category,
          visit__visit_id,
          message__name,

  lead(message__name)
        OVER (PARTITION BY visit__visit_id
          ORDER BY message__timestamp)
        AS next_page,
  lead(message__name, 2)
        OVER (PARTITION BY visit__visit_id
          ORDER BY message__timestamp)
        AS next_page2,
  lead(message__name, 3)
        OVER (PARTITION BY visit__visit_id
          ORDER BY message__timestamp)
        AS next_page3

  from asp_v_net_events

  where (partition_date BETWEEN ${hiveconf:start_date} AND ${hiveconf:end_date} )
          -- order by visit__visit_id,message__timestamp -- I removed this order by, I believe it is not needed since the leads contain orders
  ) a

group by SUBSTRING(a.partition_date,1,7) -- request was for the last 2 months aggregated to the monthly level
;
