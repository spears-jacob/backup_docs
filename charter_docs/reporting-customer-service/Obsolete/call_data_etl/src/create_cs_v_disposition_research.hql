USE ${env:ENVIRONMENT};

CREATE VIEW cs_v_disposition_research AS SELECT   cd.call_end_date_utc as call_date,
         cd.customer_type,
         cd.product,
         cd.issue_description,
         cd.cause_description,
         cd.resolution_description,
         CASE WHEN (LOWER(issue_description) RLIKE '.*billing.*')
               OR (LOWER(issue_description) RLIKE '.*payment.*')
               OR (LOWER(issue_description) RLIKE '.*credit.*')
             THEN "Billing"
             WHEN (LOWER(issue_description) RLIKE '.*account.*')
               OR (LOWER(issue_description) RLIKE '.*id management.*')
               OR (LOWER(issue_description) RLIKE '.*hierarchy.*')
               OR (LOWER(issue_description) RLIKE '.*new customer.*')
               OR (LOWER(issue_description) RLIKE '.*cbpp.*')
               OR (LOWER(issue_description) RLIKE '.*modify order.*')
             THEN "Account Management"
             WHEN (LOWER(issue_description) RLIKE '.*box.*')
               OR (LOWER(issue_description) RLIKE '.*device.*')
             THEN "Equipment"
             WHEN (LOWER(issue_description) RLIKE '.*feature.*')
               OR (LOWER(issue_description) RLIKE '.*hotelworks.*')
               OR (LOWER(issue_description) RLIKE '.*cb backup.*')
               OR (LOWER(issue_description) RLIKE '.*f-secure.*')
               OR (LOWER(issue_description) RLIKE '.*guide.*')
             THEN "Feature Issues"
             WHEN (LOWER(issue_description) RLIKE '.*quality.*')
             THEN "Quality Issues"
             WHEN (LOWER(issue_description) RLIKE '.*appointment.*')
               OR (LOWER(issue_description) RLIKE '.*completion.*')
             THEN "Appointments"
             WHEN (LOWER(issue_description) RLIKE '.*service t.*')
               OR (LOWER(issue_description) RLIKE '.*no service.*')
               OR (LOWER(issue_description) RLIKE '.*services.*')
               OR (LOWER(issue_description) RLIKE '.*service issues.*')
               OR (LOWER(issue_description) RLIKE '.*ppv issues.*')
               OR (LOWER(issue_description) RLIKE '.*video - channel.*')
               OR (LOWER(issue_description) RLIKE '.*video - music.*')
               OR (LOWER(issue_description) RLIKE '.*tech call.*')
               OR (LOWER(issue_description) RLIKE '.*self install.*')
               OR (LOWER(issue_description) RLIKE '.*activation.*')
               OR (LOWER(issue_description) RLIKE '.*ip info.*')
             THEN "Service Issues"
             WHEN (LOWER(issue_description) RLIKE '.*incomplete.*')
             THEN "Incomplete Call"
             WHEN (LOWER(issue_description) RLIKE '.*scope.*')
             THEN "Out of Scope"
             WHEN ((LOWER(issue_description) RLIKE '.*app.*')
              AND not (LOWER(issue_description) RLIKE '.*appointment.*'))
             THEN "App Issues"
             WHEN (LOWER(issue_description) RLIKE '.*general.*')
             THEN "General Question"
             WHEN (LOWER(issue_description) RLIKE '.*change of service.*')
             THEN "Change of Service"
             WHEN (LOWER(issue_description) RLIKE '.*connectivity.*')
             THEN "Connectivity"
             WHEN (LOWER(issue_description) RLIKE '.*repair.*')
             THEN "Repair"
             WHEN issue_description = '?' OR issue_description is null
             THEN 'No disposition'
             ELSE 'Other'
       END AS issue_group,                                                      -- Groups similar issues together for reporting
       CASE WHEN ((LOWER(cause_description) RLIKE '.*bill.*')
             OR (LOWER(cause_description) RLIKE '.*payment.*')
             OR (LOWER(cause_description) RLIKE '.*credit.*')
             OR (LOWER(cause_description) RLIKE '.*charge.*')
             OR (LOWER(cause_description) RLIKE '.*discount.*')
             OR (LOWER(cause_description) RLIKE '.*transactions.*')
             OR (LOWER(cause_description) RLIKE '.*coupon.*')
             OR (LOWER(cause_description) RLIKE '.*rate.*')
             OR (LOWER(cause_description) RLIKE '.*money.*')
             OR (LOWER(cause_description) RLIKE '.*offer.*')
             OR (LOWER(cause_description) RLIKE '.*promotion.*'))
             AND not (LOWER(cause_description) RLIKE '.*migrate.*')
           THEN "Billing"
           WHEN ((LOWER(cause_description) RLIKE '.*account.*')
             OR ((LOWER(cause_description) RLIKE '.*id.*')
                 AND ((LOWER(cause_description) RLIKE '.*convert.*')
                       OR (LOWER(cause_description) RLIKE '.*create.*')))
             OR (LOWER(cause_description) RLIKE '.*user.*')
             OR (LOWER(cause_description) RLIKE '.*owner.*')
             OR (LOWER(cause_description) RLIKE '.*modify existing.*')
             OR (LOWER(cause_description) RLIKE '.*login.*')
             OR (LOWER(cause_description) RLIKE '.*customer contact.*'))
             AND (not (LOWER(cause_description) RLIKE '.*bill.*')
             AND not (LOWER(cause_description) RLIKE '.*dsr.*'))
           THEN "Account Management"
           WHEN (LOWER(cause_description) RLIKE '.*email.*')
             AND (not (LOWER(cause_description) RLIKE '.*voicemail.*')
             AND NOT (LOWER(cause_description) RLIKE '.*response.*'))
           THEN "Email"
           WHEN (LOWER(cause_description) RLIKE '.*connectivity.*')
           THEN "Connectivity"
           WHEN ((LOWER(cause_description) RLIKE '.*equipment.*')
             OR (LOWER(cause_description) RLIKE '.*router.*')
             OR (LOWER(cause_description) RLIKE '.*computer.*')
             OR (LOWER(cause_description) RLIKE '.*box activation.*')
             OR (LOWER(cause_description) RLIKE '.*dvr.*')
             OR (LOWER(cause_description) RLIKE '.*remote.*')
             OR (LOWER(cause_description) RLIKE '.*accessories.*')
             OR (LOWER(cause_description) RLIKE '.*cable.*')
             OR ((LOWER(cause_description) RLIKE '.*device.*')
                 AND NOT (LOWER(cause_description) RLIKE '.*bill.*')))
             AND NOT (LOWER(cause_description) RLIKE '.*feature.*')
           THEN "Equipment"
           WHEN (LOWER(cause_description) RLIKE '.*video.*')
             AND NOT (LOWER(cause_description) RLIKE '.*guide.*')
             AND NOT (LOWER(cause_description) RLIKE '.*box.*')
             AND NOT (LOWER(cause_description) RLIKE '.*channels.*')
             AND NOT (LOWER(cause_description) RLIKE '.*service.*')
             AND NOT (LOWER(cause_description) RLIKE '.*repair.*')
             AND NOT (LOWER(cause_description) RLIKE '.*device.*')
           THEN "Video"
           WHEN ((LOWER(cause_description) RLIKE '.*feature.*')
             OR (LOWER(cause_description) RLIKE '.*caller id.*')
             OR (LOWER(cause_description) RLIKE '.*vod.*')
             OR (LOWER(cause_description) RLIKE '.*ppv.*')
             OR (LOWER(cause_description) RLIKE '.*fax.*')
             OR (LOWER(cause_description) RLIKE '.*on demand.*')
             OR (LOWER(cause_description) RLIKE '.*hotspot.*')
             OR (LOWER(cause_description) RLIKE '.*guide.*'))
             AND NOT (LOWER(cause_description) RLIKE '.*pricing.*')
             AND NOT (LOWER(cause_description) RLIKE '.*general.*')
             AND NOT (LOWER(cause_description) RLIKE '.*quality.*')
           THEN "Feature Issues"
           WHEN (LOWER(cause_description) RLIKE '.*appointment.*')
             OR (LOWER(cause_description) RLIKE '.*work order.*')
             OR (LOWER(cause_description) RLIKE '.*eta on.*')
           THEN "Appointments"
           WHEN (LOWER(cause_description) RLIKE '.*sales.*')
             OR (LOWER(cause_description) RLIKE '.*new customer.*')
             OR (LOWER(cause_description) RLIKE '.*pricing.*')
           THEN "Sales"
           WHEN ((LOWER(cause_description) RLIKE '.*service.*')
             OR (LOWER(cause_description) RLIKE '.*dial.*')
             OR (LOWER(cause_description) RLIKE '.*channels out.*')
             OR (LOWER(cause_description) RLIKE '.*calling issue.*')
             OR (LOWER(cause_description) RLIKE '.*streaming issue.*')
             OR ((LOWER(cause_description) RLIKE '.*missing.*')
                 AND (LOWER(cause_description) RLIKE '.*channel.*')))
             AND (NOT (LOWER(cause_description) RLIKE '.*device.*')
                 AND NOT (LOWER(cause_description) RLIKE '.*general.*')
                 AND NOT (LOWER(cause_description) RLIKE '.*settings.*')
                 AND NOT (LOWER(cause_description) RLIKE '.*feature.*')
                 AND NOT (LOWER(cause_description) RLIKE '.*charge.*'))
           THEN "Service Issues"
           WHEN (LOWER(cause_description) RLIKE '.*incomplete.*')
             OR (LOWER(cause_description) RLIKE '.*dead.*')
             OR (LOWER(cause_description) RLIKE '.*dropped.*')
             OR (LOWER(cause_description) RLIKE '.*csr.*')
           THEN "Incomplete Call"
           WHEN ((LOWER(cause_description) RLIKE '^existing.*')
             AND (LOWER(cause_description) RLIKE '.*customer.*'))
             OR (LOWER(cause_description) RLIKE '.*non-charter.*')
             OR (LOWER(cause_description) RLIKE '.*non-english.*')
             OR (LOWER(cause_description) RLIKE '.*consult.*')
           THEN "Out of Scope"
           WHEN (LOWER(cause_description) RLIKE '.*pixelation.*')
             OR (LOWER(cause_description) RLIKE '.*aspect.*')
             OR (LOWER(cause_description) RLIKE '.*speed.*')
             OR (LOWER(cause_description) RLIKE '.*quality.*')
           THEN "Quality Issues"
           WHEN ((LOWER(cause_description) RLIKE '.*app.*')
             AND not (LOWER(cause_description) RLIKE '.*applied.*')
             AND not (LOWER(cause_description) RLIKE '.*appear.*')
             AND not (LOWER(cause_description) RLIKE '.*login.*')
             AND not (LOWER(cause_description) RLIKE '.*appointment.*'))
           THEN "App Issues"
           WHEN ((LOWER(cause_description) RLIKE '.*general.*')
             OR (LOWER(cause_description) RLIKE '.*company information.*')
             OR (LOWER(cause_description) RLIKE '.*charter business.*')
             OR (LOWER(cause_description) RLIKE '.*spectrum business.*')
             OR (LOWER(cause_description) RLIKE '.*charter store.*'))
             AND not (LOWER(cause_description) RLIKE '.*bill.*')
             AND not (LOWER(cause_description) RLIKE '.*charge.*')
           THEN "General Question"
           WHEN (LOWER(cause_description) RLIKE '.*retention.*')
           THEN "Retention Issue"
           WHEN (((LOWER(cause_description) RLIKE '.*provisioning.*')
           OR (LOWER(cause_description) RLIKE '.*failed.*'))
           OR (LOWER(cause_description) RLIKE '.*error during.*'))
           AND NOT (LOWER(cause_description) RLIKE '.*swap.*')
           THEN "Provisioning Issue"
           WHEN (LOWER(cause_description) RLIKE '.*repair.*')
           THEN "Repair"
           WHEN cause_description = '?' OR cause_description is null
           THEN 'No disposition'
           ELSE 'Other'
       END AS cause_group,                                                      -- Groups similar causes together for reporting
       CASE WHEN ((LOWER(resolution_description) = 'customer ed')
                  AND (LOWER(issue_description) = 'account management'))
           OR ((LOWER(resolution_description) RLIKE '.*customer ed.*')
           AND NOT LOWER(resolution_description) = 'customer ed'
           AND ((LOWER(resolution_description) RLIKE '.*password.*')
           OR (LOWER(resolution_description) RLIKE '.*identit.*')
           OR (LOWER(resolution_description) RLIKE '.*head of house.*')
           OR (LOWER(resolution_description) RLIKE '.*admin.*')
           OR (LOWER(resolution_description) RLIKE '.*accept.*')
           OR (LOWER(resolution_description) RLIKE '.*user.*')
          OR (LOWER(resolution_description) RLIKE '.*account.*')))
         THEN "Customer Education - Account"
         WHEN ((LOWER(resolution_description) RLIKE '.*customer ed.*')
          AND ((LOWER(issue_description) RLIKE '.*bill.*')
          OR (LOWER(issue_description) RLIKE '.*credit.*')
          OR (LOWER(issue_description) RLIKE '.*payment.*')
          OR (LOWER(resolution_description) RLIKE '.*bill.*')
          OR (LOWER(resolution_description) RLIKE '.*coupon.*')
          OR (LOWER(resolution_description) RLIKE '.*transactions.*')
          OR (LOWER(resolution_description) RLIKE '.*payments.*')
          OR (LOWER(resolution_description) RLIKE '.*autopay.*')
          OR (LOWER(resolution_description) RLIKE '.*pricing.*')
          OR (LOWER(resolution_description) RLIKE '.*balance.*')
          OR (LOWER(resolution_description) RLIKE '.*charges.*')
          OR (LOWER(resolution_description) RLIKE '.*credit.*')
          OR (LOWER(resolution_description) RLIKE '.*rewards.*')))
         THEN "Customer Education - Billing"
         WHEN ((LOWER(resolution_description) RLIKE '.*customer ed.*')
            AND ((LOWER(resolution_description) RLIKE '.*equipment.*')
                OR (LOWER(resolution_description) RLIKE '.*device.*')
                OR ((LOWER(resolution_description) RLIKE '.*settings.*')
                  AND NOT (LOWER(resolution_description) RLIKE '.*guide.*')
                  AND NOT (LOWER(resolution_description) RLIKE '.*parental.*'))))
            OR (LOWER(resolution_description) RLIKE '.*customer equipment.*')
            OR (LOWER(resolution_description) RLIKE '.*troubleshooting equipment.*')
         THEN "Customer Education - Equipment"
         WHEN ((LOWER(resolution_description) RLIKE '.*customer ed.*')
            AND ((LOWER(resolution_description) RLIKE '.*guide.*')
                OR (LOWER(resolution_description) RLIKE '.*lineup.*')))
         THEN "Customer Education - Guide"
         WHEN (LOWER(resolution_description) = "customer education")
         THEN "Customer Education - Other"
         WHEN (LOWER(resolution_description) RLIKE '.*customer ed - service.*')
         THEN "Customer Education - Services"
         WHEN (LOWER(resolution_description) RLIKE '.*escalate.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*device.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*scheduled.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*routing.*')
         THEN "Escalated"
         WHEN (LOWER(resolution_description) RLIKE '.*transfer.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*correct.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*key.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*email.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*complete.*')
         THEN "Transferred Call"
         WHEN (LOWER(resolution_description) RLIKE '.*cycle.*')
         THEN "Power Cycled Equipment"
         WHEN (LOWER(resolution_description) RLIKE '.*truck roll.*')
         THEN "Set up Truck Roll"
         WHEN (LOWER(resolution_description) RLIKE '.*billing hit.*')
         THEN "Sent Billing Hit"
         WHEN (LOWER(resolution_description) RLIKE '.*outage.*')
         THEN "Advised of Outage"
         WHEN (LOWER(resolution_description) RLIKE '.*pay.*')
            AND ((LOWER(resolution_description) RLIKE '.*processed.*')
                OR (LOWER(resolution_description) RLIKE '.*denied.*')
                OR (LOWER(resolution_description) RLIKE '.*future.*')
                OR (LOWER(resolution_description) RLIKE '.*credit card.*'))
         THEN "Payment Processed/Arranged"
         WHEN (LOWER(resolution_description) RLIKE '.*estimated.*')
         THEN "Provided Estimated Time of Arrival"
         WHEN (LOWER(resolution_description) RLIKE '.*charter store.*')
            OR (LOWER(resolution_description) RLIKE '.*local.*')
         THEN "Referred to local store/office"
         WHEN (LOWER(resolution_description) RLIKE '.*appointment.*')
            AND NOT (LOWER(resolution_description) RLIKE '.*outage.*')
         THEN "Provided Estimated Time of Arrival"
         WHEN ((LOWER(resolution_description) RLIKE '.*added services.*')
            OR (LOWER(resolution_description) RLIKE '.*removed services.*')
            OR (LOWER(resolution_description) RLIKE '.*upgraded.*')
            OR (LOWER(resolution_description) RLIKE '.*downgrade.*'))
            AND NOT (LOWER(resolution_description) RLIKE '.*truck.*')
         THEN "Added/Removed/Upgraded/Downgraded Services"
         WHEN (LOWER(resolution_description) = "other (note required)")
         THEN "Other - Note Required"
         WHEN resolution_description = '?' OR resolution_description is null
         THEN 'No disposition'
         ELSE 'Other'
    END AS resolution_group,                                                    -- Groups similar resolutions together for reporting
         count(cd.call_id) as segments,                                         -- Count of handled segments with same disposition
         count(distinct cd.call_inbound_key) as calls,                          -- Count of unique calls with the same disposition on one of the call segments
         count(distinct cd.account_number) as accounts,                         -- Count of unique customers with the same disposition on one of the call segments
         sum(CASE WHEN cd.segment_end_timestamp_utc<cd.segment_start_timestamp_utc THEN 0 ELSE cd.segment_duration_minutes END) as talk_time  -- amount of time spent talking for segments with same disposition
FROM prod.cs_call_data cd
WHERE cd.segment_handled_flag = true                                            -- Identifies handled segments where the agent spoke with the customer
GROUP BY cd.call_end_date_utc,
         cd.customer_type,
         cd.product,
         cd.issue_description,
         cd.cause_description,
         cd.resolution_description
