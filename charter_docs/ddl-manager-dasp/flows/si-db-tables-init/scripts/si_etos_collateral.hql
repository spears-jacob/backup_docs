CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.si_etos_collateral(
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
  order_picked_date string, 
  ship_stage_date string, 
  system_ship_date string, 
  carrier_ship_date string, 
  carrier_delivery_date string, 
  code_revision string, 
  ship_to_loc string, 
  ezc_bau_facility_id string, 
  ship_from_city string, 
  ship_from_tier1 string, 
  ship_form_tier1_city string, 
  order_nbr string, 
  region string, 
  bol string, 
  dock_received_date string, 
  created_date string, 
  order_date string, 
  short_order_date string, 
  unitqty string, 
  cosys string, 
  divprin string, 
  headend string, 
  site string, 
  carrier string, 
  order_type string, 
  customer_segment string, 
  order_priority string, 
  ship_to string, 
  controller_name string, 
  tpsi_ord_type string,
  last_updated_date string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${s3_location}'
TBLPROPERTIES ( 'orc.compress'='SNAPPY')
