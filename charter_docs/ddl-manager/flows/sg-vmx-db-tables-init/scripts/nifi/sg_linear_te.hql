CREATE EXTERNAL TABLE IF NOT EXISTS nifi.sg_linear_te
(
    tun_evnt_id string,
    tun_evnt_local_strt_tm timestamp,
    tun_evnt_local_end_tm timestamp,
    utc_offset int,
    mac_add_aes256 string,
    device_id_aes256 string,
    device_type string,
    acct_num_aes256 string,
    org_lvl_1_num string,
    org_lvl_2_num string,
    org_lvl_3_num string,
    org_lvl_4_num string,
    subs_uniq_id string,
    anon_subs_id string,
    stn_id int,
    stn_aflt_nm string,
    call_sgn string,
    call_sgn_nm string,
    postal_code string,
    tun_evnt_utc_strt_time timestamp,
    tun_evnt_utc_end_time timestamp,
    display_chnl string,
    chnl_map_id string,
    controller_id string,
    src_id string,
    src_systm string,
    coll_type string,
    trans_comp_tm timestamp,
    calc_dur_sec int,
    market_name string,
    company_name string,
    input_file string
)
    PARTITIONED BY (legacy_company string, partition_date_denver string, file_date string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
