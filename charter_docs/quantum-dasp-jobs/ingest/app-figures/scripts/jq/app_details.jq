.[].product
| [
  .id, .name, .developer, .icon, .vendor_identifier, .ref_no, .sku, .package_name, .store_id, .store, .storefront, .release_date, .added_date, .updated_date, .version, .active, .source.external_account_id, .source.added_timestamp, .source.active, .source.hidden, .source.type
  ]
| @csv
