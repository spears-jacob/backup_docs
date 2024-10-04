-- set up a table
drop table if exists test.net_auto_pay;
create table test.net_auto_pay as
  SELECT 
    b.date_den as date_den, 
    b.date_time as date_time,
    b.account_number as account_number,
    b.username as username,
    b.message__name,
    b.state__view__current_page__name as page_name,
    b.state__view__current_page__section as page_section,
    b.operation__type,
    b.prev_message_name as previous_custom_link_name,
    b.prev_page as previous_page,
    b.prev_sec as previous_section,
    b.prev_operation__type as previous_additional_info,
    COUNT(*) as counts
  FROM(
    SELECT 
      date_den,
      account_number,
      date_time,
      username,
      state__view__current_page__name,
      state__view__current_page__section,
      operation__type,
      message__name,
      message__category,
      LAG(a.message__name) 
        OVER (PARTITION BY a.visit__visit_id 
          ORDER BY a.date_time)
        AS prev_message_name,
      LAG(a.state__view__current_page__name) 
        OVER (PARTITION BY a.visit__visit_id 
          ORDER BY a.date_time)
        AS prev_page,
      LAG(a.state__view__current_page__section) 
        OVER (PARTITION BY a.visit__visit_id 
          ORDER BY a.date_time)
        AS prev_sec,
      LAG(a.operation__type) 
        OVER (PARTITION BY a.visit__visit_id 
          ORDER BY a.date_time)
        AS prev_operation__type
    FROM (
      SELECT 
        epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver')as date_den,
        prod.aes_decrypt(visit__account__account_number) as account_number,
        from_unixtime(message__timestamp) as date_time,
        prod.aes_decrypt(visit__user__id) as username,
        visit__visit_id,
        state__view__current_page__name,
        state__view__current_page__section,
        operation__type,
        message__name,
        message__category
      FROM prod.net_events 
      WHERE 
        epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver') between '2017-01-01' and '2017-03-31'
        AND message__category = 'Page View'
        AND (state__view__current_page__name like 'AutoPay%'
          AND state__view__current_page__name like '%Confirm%')
          OR (
            lower(operation__type) like '%checking%'
            OR lower(operation__type) like '%saving%'
            OR lower(operation__type) like '%credit%'
          )
        ORDER BY date_time 
    ) a -- grab all page views or payments
  ) b -- list all page fields with previous section
  WHERE 
    message__category = 'Page View'
    AND (state__view__current_page__name like 'AutoPay%'
      AND state__view__current_page__name like '%Confirm%')
    AND (
      lower(prev_operation__type) like '%checking%'
      OR lower(prev_operation__type) like '%saving%'
      OR lower(prev_operation__type) like '%credit%'
    )
  GROUP BY 
    b.date_den, 
    b.account_number,
    b.date_time,
    b.username,
    b.message__name,
    b.state__view__current_page__name,
    b.state__view__current_page__section,
    b.operation__type,
    b.prev_message_name,
    b.prev_page, 
    b.prev_sec,
    b.prev_operation__type
  ;

-- Check Auto Pay Table 
SELECT
  date_den,
  count(*)
FROM test.net_auto_pay
GROUP BY
  date_den
ORDER BY
  date_den ASC
;

-- Number of Users by Payment Method
SELECT
  previous_additional_info,
  count(distinct account_number) as count_accounts,
  count(distinct username) as count_usernames
FROM test.net_auto_pay
GROUP BY
  previous_additional_info
;

--------------------------------------------------------------------------------------------------------------------

-- One Time Payments Data 
-- set up a table
drop table if exists test.net_otp;
create table test.net_otp as      
      SELECT
        epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver')as date_den,
        prod.aes_decrypt(visit__account__account_number) as account_number,
        from_unixtime(message__timestamp) as date_time,
        prod.aes_decrypt(visit__user__id) as username,
        visit__visit_id,
        CASE 
          WHEN lower(state__view__current_page__name) LIKE '%noautopay%' THEN 'No Auto-Pay'
          WHEN lower(state__view__current_page__name) LIKE '%wautopay%' THEN 'W Auto-Pay'
        END as auto_pay_status,
        CASE 
          WHEN lower(state__view__current_page__name) LIKE '%credit%' THEN 'Credit'
          WHEN lower(state__view__current_page__name) LIKE '%checking%' THEN 'Checking'
          WHEN lower(state__view__current_page__name) LIKE '%savings%' THEN 'Savings'
        END as otp_method,
        state__view__current_page__name,
        state__view__current_page__section,
        operation__type,
        message__name,
        message__category
      FROM prod.net_events 
      WHERE 
        epoch_converter(cast(message__timestamp*1000 as bigint),'America/Denver') between '2016-07-01' and '2017-03-31'
        AND message__category = 'Page View'
        AND (lower(state__view__current_page__name) LIKE '%onetime%'
          AND lower(state__view__current_page__name) LIKE '%confirm%')
          --OR (
            --lower(operation__type) like '%checking%'
            --OR lower(operation__type) like '%saving%'
            --OR lower(operation__type) like '%credit%'
          --)
        ORDER BY date_time 
;

--------------------------------------------------------------------------------------------------------------------

-- set up combined table
drop table if exists test.net_ap_otp_combined;
create table test.net_ap_otp_combined as   
  SELECT
    CASE
      WHEN ap.previous_additional_info = 'Credit/Debit' THEN 'Credit'
      WHEN ap.previous_additional_info = 'credit' THEN 'Credit'
      ELSE ap.previous_additional_info
    END AS auto_pay_method,
    ap.account_number as account_number,
    ap.date_time as ap_date_time,
    otp.otp_method as otp_otp_method,
    otp.auto_pay_status as otp_auto_pay_status,
    otp.date_time as otp_date_time
  FROM test.net_auto_pay ap
  LEFT OUTER JOIN
    (SELECT
      account_number,
      otp_method,
      auto_pay_status,
      date_time
    FROM test.net_otp 
    ) otp
  ON ap.account_number = otp.account_number
  WHERE (ap.date_time >= otp.date_time OR otp.date_time is null)
  ORDER BY
    account_number asc, 
    ap_date_time asc,
    otp_date_time asc
;

-- select all accounts that have made a AP in the month range, by AP payment method
SELECT
  auto_pay_method,
  count(distinct account_number) as count_accounts
FROM test.net_ap_otp_combined
WHERE ap_date_time is not null
GROUP BY auto_pay_method
;

-- AP only unique accounts
SELECT 
  account_number
  --COUNT(distinct account_number)
FROM test.net_ap_otp_combined
WHERE otp_date_time is null
;

-- AP only unique accounts by Method
SELECT 
  auto_pay_method,
  COUNT(distinct account_number)
FROM test.net_ap_otp_combined
WHERE otp_date_time is null
GROUP BY
  auto_pay_method
;

-- OTP Unique accounts w/ Different AP & OTP Payment Methods
SELECT
  COUNT(DISTINCT account_number)
FROM test.net_ap_otp_combined
WHERE otp_date_time is not null
  AND auto_pay_method != otp_otp_method
;

-- Segmented Data for analysis
-- split by accounts that made OTP in preceding 6 months
-- split by Payment Methods
SELECT
  CASE
    WHEN b.otp_counts = 0 THEN 'AP Only'
    WHEN b.otp_counts > 0 THEN 'AP and OTP'
    ELSE b.otp_counts
  END AS otp_status,
  a.auto_pay_method as auto_pay_method,
  a.otp_otp_method as otp_method,
  count(distinct b.account_number) as count_accounts,
  count(distinct a.ap_date_time) as count__ap_payments,
  count(distinct a.otp_date_time) as count__otp_payments
FROM test.net_ap_otp_combined a
FULL OUTER JOIN
-- Segment Accounts that made prior OTP payments
(SELECT
  account_number,
  count(distinct otp_date_time) as otp_counts
FROM test.net_ap_otp_combined
GROUP BY 
  account_number
) b
ON a.account_number = b.account_number
GROUP BY
  CASE
    WHEN b.otp_counts = 0 THEN 'AP Only'
    WHEN b.otp_counts > 0 THEN 'AP and OTP'
    ELSE b.otp_counts
  END,
  a.auto_pay_method,
  a.otp_otp_method
ORDER BY
  otp_status,
  auto_pay_method,
  otp_method
;


-- List of account #s for each group, Care Analytics team to compare this list to call volume

SELECT
  string(account_number) as acct_num,
  CASE
    WHEN otp_otp_method IS NULL THEN 'AP ONLY'
    WHEN otp_otp_method != auto_pay_method THEN 'AP & OTP DIFF METHODS'
    ELSE 'AP & OTP SAME METHOD'
  END AS payment_group,
  count(distinct ap_date_time) as num_ap_payments,
  count(distinct otp_date_time) as num__otp_payments,
  count(*) as payments
FROM test.net_ap_otp_combined
WHERE account_number IS NOT NULL
GROUP BY
  string(account_number),
  CASE
    WHEN otp_otp_method IS NULL THEN 'AP ONLY'
    WHEN otp_otp_method != auto_pay_method THEN 'AP & OTP DIFF METHODS'
    ELSE 'AP & OTP SAME METHOD'
  END
;