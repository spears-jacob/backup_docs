#!/bin/bash
echo "Loading partitions in ${ACCOUNTATOM}"
hive -v -e "MSCK REPAIR TABLE ${ACCOUNTATOM};"

echo "Creating view in ${DASP_db}"
hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.red_atom_snapshot_accounts_v AS
SELECT
      charter_account_commercial_subtype,
      encrypted_legacy_account_id,
      encrypted_legacy_account_id_256,
      encrypted_legacy_account_number_256,
      legacy_account_type,
      legacy_account_type_code,
      account_type,
      account_type_code,
      account_type_exclusion_description,
      bulk_internet_details,
      company,
      location_state,
      encrypted_billing_customer_name_256,
      encrypted_billing_address_line_1_256,
      encrypted_billing_address_line_2_256,
      billing_state,
      encrypted_home_phone_number,
      encrypted_home_phone_number_256,
      encrypted_other_phone_number_256,
      encrypted_business_phone_number_256,
      advertisement_zone_name,
      billable_unit_count,
      is_commercial_business_subscriber,
      customer_connect_count,
      customer_connect_date,
      customer_migration_channel,
      customer_migration_date_eastern,
      customer_migration_month_eastern,
      customer_migration_type,
      customer_type,
      designated_market_area,
      development_code,
      device_type_name,
      encrypted_digital_phone_number,
      customer_disconnect_count,
      customer_disconnect_date,
      docsis_code,
      doors_subscriber_count,
      customer_downgrade_count,
      dwelling_type_code,
      dwelling_type_description,
      fta_description,
      gigabyte_service_description,
      headend_code,
      headend_hsl_code,
      headend_description,
      headend_id,
      headend_name,
      headend_state,
      hierarchy_id,
      encrypted_legacy_house_source_key_256,
      hub_number,
      install_date_eastern,
      internet_sales_channel,
      internet_connect_count,
      internet_disconnect_count,
      internet_downgrade_count,
      internet_price_usd,
      internet_speed_down_mbps,
      internet_speed_up_mbps,
      internet_subscriber_count,
      internet_tier,
      internet_upgrade_count,
      has_accessibility,
      is_all_invoice_line_of_business,
      employee_indicator,
      has_basic_services,
      has_battery_backup,
      is_bcn,
      is_cbi,
      has_california_lifeline_phone_credit,
      has_coax,
      has_intelligent_home,
      has_internet_package,
      has_mexico_200_voice_plan,
      has_mexico_500_voice_plan,
      has_ported_phone_line,
      has_private_phone_listing,
      has_second_phone_line,
      is_small_medium_business,
      has_spectrum_business_pricing_plan,
      has_spectrum_guide,
      has_spectrum_internet_assist_plan,
      has_spectrum_packaging_pricing_choice,
      has_spectrum_packaging_pricing_internet,
      has_spectrum_packaging_pricing_video_line_of_business,
      has_spectrum_packaging_pricing,
      has_talking_guide,
      is_basic_tenant,
      is_tenant,
      has_promotion,
      has_unlimited_interstate_voice_plan,
      has_unlimited_intrastate_voice_plan,
      has_unlimited_mexico_voice_plan,
      has_video_on_demand,
      has_video_package,
      has_video_stream_choice_package,
      has_video_stream_package,
      has_voice_package,
      has_voice_spectrum_packaging_pricing_package,
      has_voice_spectrum_packaging_pricing_internet,
      has_worldwide_250_voice_plan,
      nielsen_designated_market_area,
      key_market_area_description,
      key_market_area_id,
      lineup_description,
      lineup_id,
      manufacturer,
      market,
      model,
      package_category,
      premium_stream_service_name,
      promotion_description,
      region,
      customer_sidegrade_count,
      spectrum_spp_type,
      stream_video_description,
      technician_contracting_firm_name,
      technician_id,
      encrypted_technician_first_name_256,
      encrypted_technician_last_name_256,
      total_price_after_discount_usd,
      total_price_before_discount_usd,
      truck_trouble_call_count,
      customer_upgrade_count,
      video_sales_channel,
      video_connect_count,
      video_disconnect_count,
      video_upgrade_count,
      video_downgrade_count,
      has_fiber_to_premises,
      video_fiber_node,
      video_package_type,
      prior_video_package_type,
      video_price_usd,
      video_stream_premium_service_description,
      video_subscriber_count,
      voice_connect_count,
      voice_disconnect_count,
      voice_downgrade_count,
      voice_sales_channel,
      voice_price_usd,
      voice_subscriber_count,
      voice_upgrade_count,
      wireless_gateway_count,
      encrypted_work_phone_number_256,
      wifi_service_price_before_discount_usd,
      wifi_service_price_after_discount_usd,
      has_pay_wifi,
      has_wifi_public_ssid_opt_in,
      has_wifi,
      has_spectrum_wifi_opt_in,
      has_static_ip,
      commercial_business_clear_qam_unit_count,
      commercial_business_clear_qam_analog_unit_count,
      commercial_business_clear_qam_qam_unit_count,
      has_enterprise_hosted_voice,
      smb_video_subscriber_count,
      smb_internet_subscriber_count,
      smb_voice_subscriber_count,
      billing_system,
      encrypted_legacy_account_number,
      billing_vendor,
      has_sbpp_or_npp_wifi_plan,
      has_small_medium_business_wifi,
      has_commercial_business_wifi,
      has_recurring_payment,
      has_cloud_dvr,
      is_from_tam_tool_test,
      is_from_tam_tool_extract,
      internet_product_tier,
      encrypted_account_key_256,
      encrypted_system_256,
      encrypted_prin_256,
      encrypted_agent_256,
      encrypted_division_256,
      encrypted_franchise_256,
      encrypted_franchise_agent_code_256,
      encrypted_location_city_256,
      encrypted_location_zip_code_256,
      encrypted_location_zip_code_4_256,
      encrypted_billing_city_256,
      encrypted_billing_zip_code_256,
      encrypted_billing_zip_code_4_256,
      encrypted_house_development_number_256,
      encrypted_premises_key_256,
      encrypted_source_house_key_256,
      encrypted_transfer_account_key_256,
      encrypted_padded_system_256,
      encrypted_padded_prin_256,
      encrypted_padded_agent_256,
      encrypted_normalized_home_phone_number_256,
      encrypted_normalized_other_phone_number_256,
      encrypted_normalized_business_phone_number_256,
      encrypted_normalized_work_phone_number_256,
      encrypted_billing_slice_256,
      encrypted_division_id_256,
      encrypted_padded_account_number_256,
      encrypted_unique_franchise_id_256,
      is_enterprise_customer,
      row_last_updated,
      bulk_video_details,
      is_bulk_tenant,
      odn_guide_count,
      spectrum_guide_count,
      stb_guide_type,
      partition_date_denver,
      account_status,
      extract_source,
      extract_type,
      legacy_company,
      CASE
       WHEN legacy_company='CHR' AND partition_date_denver > '2019-06-05' THEN controller_name
       WHEN legacy_company<>'CHR' AND partition_date_denver > '2019-07-29' THEN controller_name
       ELSE CAST(null AS string)
      END AS controller_name,
      CASE
      WHEN row_last_updated >= '2020-02-04' THEN has_bulk_video
      ELSE CAST(null AS boolean)
      END AS has_bulk_video,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN has_bulk_internet
      ELSE CAST(null AS boolean)
      END AS has_bulk_internet,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN has_bulk_voice
      ELSE CAST(null AS boolean)
      END AS has_bulk_voice,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN bulk_master_video_count
      ELSE CAST(null AS int)
      END AS bulk_master_video_count,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN bulk_master_internet_count
      ELSE CAST(null AS int)
      END AS bulk_master_internet_count,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN bulk_master_voice_count
      ELSE CAST(null AS int)
      END AS bulk_master_voice_count,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN has_tenant_video
      ELSE CAST(null AS boolean)
      END AS has_tenant_video,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN has_tenant_internet
      ELSE CAST(null AS boolean)
      END AS has_tenant_internet,

      CASE
      WHEN row_last_updated >= '2020-02-04' THEN has_tenant_voice
      ELSE CAST(null AS boolean)
      END AS has_tenant_voice
FROM ${ACCOUNTATOM};"

echo "Checking available dates in ${DASP_db}.red_atom_snapshot_accounts_v"
hive -v -e "SELECT DISTINCT partition_date_denver
FROM ${DASP_db}.red_atom_snapshot_accounts_v
WHERE partition_date_denver >= '${START_DATE_DENVER}'
ORDER BY partition_date_denver desc
LIMIT 10;"
