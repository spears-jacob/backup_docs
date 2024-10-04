USE ${env:LKP_db};

ALTER TABLE nifi_sbnet_twc_event DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_browser DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_country DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_connection_type DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_javascript DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_language DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_os DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_resolution DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_twc_search_engine DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
