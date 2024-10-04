# Unique counts metrics

This includes all unique counts .

## Filters
All unique counts metrics has the following filters:
- With MSOs assigned: `visit__account__details__mso <> '' and isnotnull(visit__account__details__mso)`

## Grouping Levels
All playback metrics has the following grouping levels:
- MSO. `MSO-MISSING` should be all filtered out if filter is applied
```
CASE
  WHEN visit__account__details__mso IS NULL THEN 'MSO-MISSING'
  WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
  WHEN visit__account__details__mso = '' THEN 'MSO-MISSING'
  WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
  WHEN visit__account__details__mso = 'BH' THEN 'L-BHN'
  WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
  ELSE visit__account__details__mso
END as mso
```
- Application Type:
  - without adroid version breakdown: `visit__application_details__application_type as application_type`,
  - or with android version break down:
```
CASE
  WHEN visit__application_details__application_type = 'Android' THEN (
    case when visit__device__operating_system LIKE '%4.0%' OR visit__device__operating_system LIKE '%4.1%' OR visit__device__operating_system LIKE '%4.2%' OR visit__device__operating_system LIKE '%4.3%' THEN 'Android 4.1 - 4.3' ELSE 'Android 4.4+'
    END) ELSE visit__application_details__application_type
END as application_type
```
- Device type: `lower(visit__device__device_type) as device_type`
- Connection Type: `visit__connection__connection_type as connection_type`
- Network Status: `visit__connection__network_status as network_status`
- Playback Type: `state__content__stream__playback_type as playback_type`

## Calculation

- unique_devices, `SIZE(COLLECT_SET(device_id))`, and device_id is provided by: `if((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat', visit__device__uuid, NULL) AS device_id`
- unique_households, `SIZE(COLLECT_SET(account_number))`, and account_number is: `if((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat', visit__account__account_number, NULL) AS account_number`
- unique_visits, `SIZE(COLLECT_SET(visit_id))`, and visit_id is: `if((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat', visit__visit_id, NULL) AS visit_id`
- set_unique_streams, `size(COLLECT_SET(stream_id))` and stream_id is filtered down to streams with playbackStart: `if(message__name = 'playbackStart', state__content__stream__playback_id, NULL) AS stream_id`. 
