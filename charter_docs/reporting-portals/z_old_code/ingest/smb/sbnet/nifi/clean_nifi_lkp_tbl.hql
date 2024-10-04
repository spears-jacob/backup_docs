USE ${env:LKP_db};

ALTER TABLE nifi_sbnet_event DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_browser DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_country DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_connection_type DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_javascript DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_language DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_os DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_resolution DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_search_engine DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
