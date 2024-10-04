USE ${env:LKP_db};

ALTER TABLE nifi_sbnet_dev_event DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_browser DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_country DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_connection_type DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_javascript DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_language DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_os DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_resolution DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
ALTER TABLE nifi_sbnet_dev_search_engine DROP IF EXISTS PARTITION(partition_date > '0') PURGE;
