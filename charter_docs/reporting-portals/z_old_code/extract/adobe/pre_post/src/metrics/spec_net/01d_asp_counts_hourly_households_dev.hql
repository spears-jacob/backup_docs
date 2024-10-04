set hive.exec.max.dynamic.partitions=20000
;
SET hive.exec.max.dynamic.partitions.pernode=7000
;

USE ${env:ENVIRONMENT}
;

INSERT OVERWRITE TABLE asp_counts_hourly
PARTITION(unit,platform,domain,company,date_denver,source_table)
SELECT
    COUNT(DISTINCT(visit__account__enc_account_number)) as value,
--    NULL AS browser_size_breakpoint,
    CASE
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-internet-banner')
            THEN 'CL-55|EQUIPMENT|From Homepage Banner|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-tv-banner')
            THEN 'CL-55|EQUIPMENT|From Homepage Banner|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-voice-banner')
            THEN 'CL-55|EQUIPMENT|From Homepage Banner|Voice|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('localnav-internet')
            THEN 'CL-55|EQUIPMENT|From Local Nav|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('localnav-tv')
            THEN 'CL-55|EQUIPMENT|From Local Nav|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('localnav-voice')
            THEN 'CL-55|EQUIPMENT|From Local Nav|Voice|'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('my-internet-services')
            AND LOWER(visit__settings['post_prop9']) RLIKE '.*support.*'
            THEN 'CL-55|EQUIPMENT|From Support|Internet|'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('my-tv-services')
            AND LOWER(visit__settings['post_prop9']) RLIKE '.*support.*'
            THEN 'CL-55|EQUIPMENT|From Support|TV|'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('my-voice-services')
            AND LOWER(visit__settings['post_prop9']) RLIKE '.*support.*'
            THEN 'CL-55|EQUIPMENT|From Support|Voice|'
        WHEN message__category <> 'Page View'
            AND LOWER(message__name) = 'refresh'
            THEN 'CL-55|EQUIPMENT|Reset Start|TV|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'equip.tv.troubleshoot.reset-equip.start'
            THEN 'CL-55|EQUIPMENT|Reset Start|TV|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'refresh-status'
            AND LOWER(operation__type) = 'success'
            THEN 'CL-55|EQUIPMENT|Reset Complete|TV|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'reset-equipment-tv-success'
            THEN 'CL-55|EQUIPMENT|Reset Complete|TV|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('reset-equipment-wifi','reset-modem-internetvoice')
            THEN 'CL-55|EQUIPMENT|Reset Start|Internet|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'equip.internet.troubleshoot.reset-equip.start'
            THEN 'CL-55|EQUIPMENT|Reset Start|Internet|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('test-connection-wifi','test-connection-internetvoice','test-connection-internet')
            THEN 'CL-55|EQUIPMENT|Reset Complete|Internet|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'reset-equipment-internet-success'
            THEN 'CL-55|EQUIPMENT|Reset Complete|Internet|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('reset-modem-voice')
            THEN 'CL-55|EQUIPMENT|Reset Start|Voice|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'equip.voice.troubleshoot.reset-equip.start'
            THEN 'CL-55|EQUIPMENT|Reset Start|Voice|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('test-connection-voice', 'test-connection-internetvoice')
            THEN 'CL-55|EQUIPMENT|Reset Complete|Voice|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('reset-equipment-voice-success')
            THEN 'CL-55|EQUIPMENT|Reset Complete|Voice|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum','ask-spectrum')
            AND state__view__current_page__name = 'my-tv-services'
            THEN 'CL-55|EQUIPMENT|Ask Spectrum|from My Tv Services|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum','ask-spectrum')
            AND state__view__current_page__name = 'my-internet-services'
            THEN 'CL-55|EQUIPMENT|Ask Spectrum|from My Internet Services|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum','ask-spectrum')
            AND state__view__current_page__name = 'my-voice-services'
            THEN 'CL-55|EQUIPMENT|Ask Spectrum|from My Voice Services|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.upgrade-service')
            THEN 'CL-55|EQUIPMENT|Upgrade Service|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.upgrade-service')
            THEN 'CL-55|EQUIPMENT|Upgrade Service|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.upgrade-service')
            THEN 'CL-55|EQUIPMENT|Upgrade Service|Voice|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.view-troubleshooting-video')
            THEN 'CL-55|EQUIPMENT|View Support Video|TV|view-troubleshooting-video'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.view-more-videos')
            THEN 'CL-55|EQUIPMENT|View Support Video|TV|view-more-videos'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.view-troubleshooting-video')
            THEN 'CL-55|EQUIPMENT|View Support Video|Internet|view-troubleshooting-video'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.view-more-videos')
            THEN 'CL-55|EQUIPMENT|View Support Video|Internet|view-more-videos'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.view-troubleshooting-video')
            THEN 'CL-55|EQUIPMENT|View Support Video|Voice|view-troubleshooting-video'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.view-more-videos')
            THEN 'CL-55|EQUIPMENT|View Support Video|Voice|view-more-videos'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.expand-device')
            THEN 'CL-55|EQUIPMENT|View Equipment Details|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.expand-device')
            THEN 'CL-55|EQUIPMENT|View Equipment Details|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.expand-device')
            THEN 'CL-55|EQUIPMENT|View Equipment Details|Voice|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.enlarge-device-image')
            THEN 'CL-55|EQUIPMENT|View Equipment Image|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.enlarge-device-image')
            THEN 'CL-55|EQUIPMENT|View Equipment Image|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.enlarge-device-image')
            THEN 'CL-55|EQUIPMENT|View Equipment Image|Voice|'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('onetime-noautopay', 'onetime-wautopay')
            THEN 'CL-55|BILLING|One Time Payment|step1|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('onetime-noautopay-credit-review', 'onetime-noautopay-checking-review', 'onetime-noautopay-savings-review', 'onetime-wautopay-credit-review  ', 'onetime-wautopay-checking-review', 'onetime-wautopay-savings-review')
            THEN 'CL-55|BILLING|One Time Payment|step2|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('onetime-noautopay-credit-confirm', 'onetime-noautopay-checking-confirm', 'onetime-noautopay-savings-confirm', 'onetime-wautopay-credit-confirm', 'onetime-wautopay-checking-confirm', 'onetime-wautopay-savings-confirm')
            THEN 'CL-55|BILLING|One Time Payment|confirmation|Pre'
        WHEN message__category = 'Page View'
            AND (LOWER(message__name) IN ('pay-bill.onetime') or (LOWER(visit__settings["post_prop11"]) = 'pay-bill.make-a-payment-hp' and LOWER(state__view__current_page__name) = 'billing-and-transactions'))
            THEN 'CL-55|BILLING|One Time Payment|step1|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-payment-date')
            THEN 'CL-55|BILLING|One Time Payment|step2|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-payment-method')
            THEN 'CL-55|BILLING|One Time Payment|step3|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-review-autopay-option', 'pay-bill.onetime-review-no-autopay-option')
            THEN 'CL-55|BILLING|One Time Payment|step4|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-confirmation', 'pay-bill.onetime-confirmation-with-autopay-enrollment')
            THEN 'CL-55|BILLING|One Time Payment|confirmation|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('autopay-wbalance','autopay-nobalance')
            THEN 'CL-55|BILLING|AutoPay|step1|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('autopay-wbalance-credit-review','autopay-nobalance-credit-review','autopay-wbalance-checking-review','autopay-nobalance-checking-review','autopay-wbalance-savings-review','autopay-nobalance-savings-review')
            THEN 'CL-55|BILLING|AutoPay|step2|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('autopay-nobalance-savings-confirm','autopay-wbalance-savings-confirm','autopay-nobalance-checking-confirm','autopay-nobalance-credit-confirm','autopay-wbalance-checking-confirm','autopay-wbalance-credit-confirm')
            THEN 'CL-55|BILLING|AutoPay|confirmation|Pre'
        WHEN message__category = 'Page View'
            AND (LOWER(message__name) IN ('pay-bill.autopay-enrollment') or (LOWER(visit__settings["post_prop11"]) = 'pay-bill.enroll-in-autopay-hp' and LOWER(state__view__current_page__name) = 'billing-and-transactions'))
            THEN 'CL-55|BILLING|AutoPay|step1|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.autopay-enrollment-confirmation')
            THEN 'CL-55|BILLING|AutoPay|confirmation|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum')
            AND (state__view__current_page__page_id RLIKE ('\.net\/payment\/') or state__view__current_page__page_id RLIKE ('\.net\/billing-and-transactions\/'))
            THEN 'CL-55|BILLING|Ask Spectrum|from Billing|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum')
            AND (visit__settings['post_prop23'] = 'billing')
            THEN 'CL-55|BILLING|Ask Spectrum|from Billing|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'home'
            AND visit__isp__status='Logged Out'
            THEN 'CL-53|HomePage Release|Home|Unauth|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'home-unauth'
            THEN 'CL-53|HomePage Release|Home|Unauth|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(state__view__current_page__name) = 'login'
            AND message__name RLIKE 'Sign-| In'
            THEN 'CL-53|HomePage Release|Sign In|Unauth|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(state__view__current_page__name) = 'home-unauth'
            AND message__name RLIKE 'Sign-| In'
            THEN 'CL-53|HomePage Release|Sign In|Unauth|Post'
        WHEN message__category = 'Page View'
            AND state__view__current_page__name IN ('pay-bill.enroll-in-autopay-no-balance.credit','pay-bill.enroll-in-autopay-no-balance.checking','pay-bill.enroll-in-autopay-no-balance.savings','pay-bill.enroll-in-autopay-with-balance.credit','pay-bill.enroll-in-autopay-with-balance.checking','pay-bill.enroll-in-autopay-with-balance.savings')
            THEN 'CL-53|HomePage Release|AutoPay|confirmation|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-overview-banner')
            THEN 'CL-53|HomePage Release|Upgrade Links|Overview|'
        WHEN message__category = 'Custom Link'
            AND state__view__current_page__name = 'home-authenticated'
            AND message__name =  'localNav-billing'
            THEN 'CL-53|HomePage Release|Bill Pay Clicks|From Local Nav|'
        WHEN message__category = 'Custom Link'
            AND message__name in ('pay-bill.make-a-payment-hp','pay-bill.enroll-in-autopay-hp','pay-bill.view-bill-details-hp')
            THEN 'CL-53|HomePage Release|Bill Pay Clicks|From Bill Pay Card on HP|'
        ELSE 'Other'
    END as metric,
    epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_hour_denver,
    'households' as unit,
    'ASP' AS platform,
    'resi' AS domain,
    'L-CHTR' AS company,
    epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver,
    'net_dev_events' AS source_table
FROM
    asp_v_net_dev_events
WHERE
    (epoch_datehour(cast(message__timestamp*1000 as bigint),'UTC') >= ("${env:START_DATE_TZ}") AND epoch_datehour(cast(message__timestamp*1000 as bigint),'UTC') < ("${env:END_DATE_TZ}"))
GROUP BY
    epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
    epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver'),
    CASE
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-internet-banner')
            THEN 'CL-55|EQUIPMENT|From Homepage Banner|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-tv-banner')
            THEN 'CL-55|EQUIPMENT|From Homepage Banner|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-voice-banner')
            THEN 'CL-55|EQUIPMENT|From Homepage Banner|Voice|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('localnav-internet')
            THEN 'CL-55|EQUIPMENT|From Local Nav|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('localnav-tv')
            THEN 'CL-55|EQUIPMENT|From Local Nav|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('localnav-voice')
            THEN 'CL-55|EQUIPMENT|From Local Nav|Voice|'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('my-internet-services')
            AND LOWER(visit__settings['post_prop9']) RLIKE '.*support.*'
            THEN 'CL-55|EQUIPMENT|From Support|Internet|'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('my-tv-services')
            AND LOWER(visit__settings['post_prop9']) RLIKE '.*support.*'
            THEN 'CL-55|EQUIPMENT|From Support|TV|'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('my-voice-services')
            AND LOWER(visit__settings['post_prop9']) RLIKE '.*support.*'
            THEN 'CL-55|EQUIPMENT|From Support|Voice|'
        WHEN message__category <> 'Page View'
            AND LOWER(message__name) = 'refresh'
            THEN 'CL-55|EQUIPMENT|Reset Start|TV|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'equip.tv.troubleshoot.reset-equip.start'
            THEN 'CL-55|EQUIPMENT|Reset Start|TV|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'refresh-status'
            AND LOWER(operation__type) = 'success'
            THEN 'CL-55|EQUIPMENT|Reset Complete|TV|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'reset-equipment-tv-success'
            THEN 'CL-55|EQUIPMENT|Reset Complete|TV|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('reset-equipment-wifi','reset-modem-internetvoice')
            THEN 'CL-55|EQUIPMENT|Reset Start|Internet|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'equip.internet.troubleshoot.reset-equip.start'
            THEN 'CL-55|EQUIPMENT|Reset Start|Internet|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('test-connection-wifi','test-connection-internetvoice','test-connection-internet')
            THEN 'CL-55|EQUIPMENT|Reset Complete|Internet|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'reset-equipment-internet-success'
            THEN 'CL-55|EQUIPMENT|Reset Complete|Internet|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('reset-modem-voice')
            THEN 'CL-55|EQUIPMENT|Reset Start|Voice|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) = 'equip.voice.troubleshoot.reset-equip.start'
            THEN 'CL-55|EQUIPMENT|Reset Start|Voice|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('test-connection-voice', 'test-connection-internetvoice')
            THEN 'CL-55|EQUIPMENT|Reset Complete|Voice|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) IN ('reset-equipment-voice-success')
            THEN 'CL-55|EQUIPMENT|Reset Complete|Voice|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum','ask-spectrum')
            AND state__view__current_page__name = 'my-tv-services'
            THEN 'CL-55|EQUIPMENT|Ask Spectrum|from My Tv Services|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum','ask-spectrum')
            AND state__view__current_page__name = 'my-internet-services'
            THEN 'CL-55|EQUIPMENT|Ask Spectrum|from My Internet Services|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum','ask-spectrum')
            AND state__view__current_page__name = 'my-voice-services'
            THEN 'CL-55|EQUIPMENT|Ask Spectrum|from My Voice Services|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.upgrade-service')
            THEN 'CL-55|EQUIPMENT|Upgrade Service|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.upgrade-service')
            THEN 'CL-55|EQUIPMENT|Upgrade Service|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.upgrade-service')
            THEN 'CL-55|EQUIPMENT|Upgrade Service|Voice|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.view-troubleshooting-video')
            THEN 'CL-55|EQUIPMENT|View Support Video|TV|view-troubleshooting-video'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.view-more-videos')
            THEN 'CL-55|EQUIPMENT|View Support Video|TV|view-more-videos'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.view-troubleshooting-video')
            THEN 'CL-55|EQUIPMENT|View Support Video|Internet|view-troubleshooting-video'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.view-more-videos')
            THEN 'CL-55|EQUIPMENT|View Support Video|Internet|view-more-videos'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.view-troubleshooting-video')
            THEN 'CL-55|EQUIPMENT|View Support Video|Voice|view-troubleshooting-video'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.view-more-videos')
            THEN 'CL-55|EQUIPMENT|View Support Video|Voice|view-more-videos'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.expand-device')
            THEN 'CL-55|EQUIPMENT|View Equipment Details|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.expand-device')
            THEN 'CL-55|EQUIPMENT|View Equipment Details|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.expand-device')
            THEN 'CL-55|EQUIPMENT|View Equipment Details|Voice|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.tv.enlarge-device-image')
            THEN 'CL-55|EQUIPMENT|View Equipment Image|TV|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.internet.enlarge-device-image')
            THEN 'CL-55|EQUIPMENT|View Equipment Image|Internet|'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('equipment.voice.enlarge-device-image')
            THEN 'CL-55|EQUIPMENT|View Equipment Image|Voice|'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('onetime-noautopay', 'onetime-wautopay')
            THEN 'CL-55|BILLING|One Time Payment|step1|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('onetime-noautopay-credit-review', 'onetime-noautopay-checking-review', 'onetime-noautopay-savings-review', 'onetime-wautopay-credit-review  ', 'onetime-wautopay-checking-review', 'onetime-wautopay-savings-review')
            THEN 'CL-55|BILLING|One Time Payment|step2|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('onetime-noautopay-credit-confirm', 'onetime-noautopay-checking-confirm', 'onetime-noautopay-savings-confirm', 'onetime-wautopay-credit-confirm', 'onetime-wautopay-checking-confirm', 'onetime-wautopay-savings-confirm')
            THEN 'CL-55|BILLING|One Time Payment|confirmation|Pre'
        WHEN message__category = 'Page View'
            AND (LOWER(message__name) IN ('pay-bill.onetime') or (LOWER(visit__settings["post_prop11"]) = 'pay-bill.make-a-payment-hp' and LOWER(state__view__current_page__name) = 'billing-and-transactions'))
            THEN 'CL-55|BILLING|One Time Payment|step1|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-payment-date')
            THEN 'CL-55|BILLING|One Time Payment|step2|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-payment-method')
            THEN 'CL-55|BILLING|One Time Payment|step3|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-review-autopay-option', 'pay-bill.onetime-review-no-autopay-option')
            THEN 'CL-55|BILLING|One Time Payment|step4|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.onetime-confirmation', 'pay-bill.onetime-confirmation-with-autopay-enrollment')
            THEN 'CL-55|BILLING|One Time Payment|confirmation|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('autopay-wbalance','autopay-nobalance')
            THEN 'CL-55|BILLING|AutoPay|step1|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('autopay-wbalance-credit-review','autopay-nobalance-credit-review','autopay-wbalance-checking-review','autopay-nobalance-checking-review','autopay-wbalance-savings-review','autopay-nobalance-savings-review')
            THEN 'CL-55|BILLING|AutoPay|step2|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('autopay-nobalance-savings-confirm','autopay-wbalance-savings-confirm','autopay-nobalance-checking-confirm','autopay-nobalance-credit-confirm','autopay-wbalance-checking-confirm','autopay-wbalance-credit-confirm')
            THEN 'CL-55|BILLING|AutoPay|confirmation|Pre'
        WHEN message__category = 'Page View'
            AND (LOWER(message__name) IN ('pay-bill.autopay-enrollment') or (LOWER(visit__settings["post_prop11"]) = 'pay-bill.enroll-in-autopay-hp' and LOWER(state__view__current_page__name) = 'billing-and-transactions'))
            THEN 'CL-55|BILLING|AutoPay|step1|Post'
        WHEN message__category = 'Page View'
            AND LOWER(message__name) IN ('pay-bill.autopay-enrollment-confirmation')
            THEN 'CL-55|BILLING|AutoPay|confirmation|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum')
            AND (state__view__current_page__page_id RLIKE ('\.net\/payment\/') or state__view__current_page__page_id RLIKE ('\.net\/billing-and-transactions\/'))
            THEN 'CL-55|BILLING|Ask Spectrum|from Billing|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('open-ask-spectrum')
            AND (visit__settings['post_prop23'] = 'billing')
            THEN 'CL-55|BILLING|Ask Spectrum|from Billing|Post'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'home'
            AND visit__isp__status='Logged Out'
            THEN 'CL-53|HomePage Release|Home|Unauth|Pre'
        WHEN message__category = 'Page View'
            AND LOWER(state__view__current_page__name) = 'home-unauth'
            THEN 'CL-53|HomePage Release|Home|Unauth|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(state__view__current_page__name) = 'login'
            AND message__name RLIKE 'Sign-| In'
            THEN 'CL-53|HomePage Release|Sign In|Unauth|Pre'
        WHEN message__category = 'Custom Link'
            AND LOWER(state__view__current_page__name) = 'home-unauth'
            AND message__name RLIKE 'Sign-| In'
            THEN 'CL-53|HomePage Release|Sign In|Unauth|Post'
        WHEN message__category = 'Page View'
            AND state__view__current_page__name IN ('pay-bill.enroll-in-autopay-no-balance.credit','pay-bill.enroll-in-autopay-no-balance.checking','pay-bill.enroll-in-autopay-no-balance.savings','pay-bill.enroll-in-autopay-with-balance.credit','pay-bill.enroll-in-autopay-with-balance.checking','pay-bill.enroll-in-autopay-with-balance.savings')
            THEN 'CL-53|HomePage Release|AutoPay|confirmation|Post'
        WHEN message__category = 'Custom Link'
            AND LOWER(message__name) IN ('upgrade-overview-banner')
            THEN 'CL-53|HomePage Release|Upgrade Links|Overview|'
        WHEN message__category = 'Custom Link'
            AND state__view__current_page__name = 'home-authenticated'
            AND message__name =  'localNav-billing'
            THEN 'CL-53|HomePage Release|Bill Pay Clicks|From Local Nav|'
        WHEN message__category = 'Custom Link'
            AND message__name in ('pay-bill.make-a-payment-hp','pay-bill.enroll-in-autopay-hp','pay-bill.view-bill-details-hp')
            THEN 'CL-53|HomePage Release|Bill Pay Clicks|From Bill Pay Card on HP|'
        ELSE 'Other'
    END
HAVING
    metric <> 'Other'
;
