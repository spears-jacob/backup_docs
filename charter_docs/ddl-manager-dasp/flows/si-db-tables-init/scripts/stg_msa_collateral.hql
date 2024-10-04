CREATE EXTERNAL TABLE ${db_name}.stg_msa_collateral(
  billing_account_number string, 
  billing_order_number string, 
  jde_item string, 
  manufacturer_name string, 
  head_end_component string, 
  platform string, 
  unit_type string, 
  blling_model string, 
  edw_standard string, 
  serial_nbr string, 
  pallet_nbr string, 
  component_nbr string, 
  picked_user string, 
  shipped_user string, 
  order_picked_date string, 
  ship_stage_date string, 
  system_ship_date string, 
  carrier_ship_date string, 
  carrier_delivery_date string, 
  code_revision string, 
  ship_to_loc string, 
  street_addr string, 
  city string, 
  state string, 
  location string, 
  ezc_bau_facility_id string, 
  ship_from_city string, 
  ship_from_tier1 string, 
  ship_form_tier1_city string, 
  order_nbr string, 
  mac_ua string, 
  region string, 
  bol string, 
  dock_received_date string, 
  created_date string, 
  order_date string, 
  unitqty string, 
  cosys string, 
  divprin string, 
  headend string, 
  site string, 
  carrier string, 
  tracking string, 
  order_type string, 
  customer_segment string, 
  order_priority string, 
  ship_to string, 
  controller_name string, 
  shipto_zip string, 
  tpsi_ord_type string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://pi-qtm-si-prod-aggregates-pii/data/tmp_prod_dasp/msa_collateral'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1608604372')