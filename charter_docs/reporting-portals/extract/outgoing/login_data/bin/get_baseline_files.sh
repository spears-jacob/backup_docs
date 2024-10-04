export FILE_NAME=asp_extract_login_data_baseline_pvt_2020-06-15_spa

hive -e "set hive.cli.print.header=false;
             USE prod;
             SELECT
                    REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+','') as account_number,
                    max(myspectrum) as myspectrum,
                    max(specnet) as specnet,
                    max(spectrumbusiness) as spectrumbusiness,
                    max(specmobile) as specmoapp,
                    max(biller_type) as biller_type,
                    max(date_denver) as date_denver,
                    prod.aes_decrypt256(division) as division,
                    prod.aes_decrypt256(division_id) as division_id,
                    max(prod.aes_decrypt256(site_sys)) as sys,
                    max(prod.aes_decrypt256(prn)) as prn,
                    max(prod.aes_decrypt256(agn)) as agn,
                    max(prod.aes_decrypt256(acct_site_id)) as acct_site_id,
                    max(prod.aes_decrypt256(acct_company)) as acct_company,
                    max(prod.aes_decrypt256(acct_franchise)) as acct_franchise
             FROM asp_extract_login_data_baseline_pvt
            WHERE length(REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+','')) between 1 and 16
             group by date_denver,
                      REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+',''),
                      prod.aes_decrypt256(division),
                      prod.aes_decrypt256(division_id);
            " | sed 's/NULL//g' > $FILE_NAME.tsv

tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz  $FILE_NAME.ts

export FILE_NAME=asp_extract_login_data_daily_2months_pvt_2020-06-15_spa

hive -e "set hive.cli.print.header=false;
             USE prod;
             SELECT
                    REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+','') as account_number,
                    max(myspectrum) as myspectrum,
                    max(specnet) as specnet,
                    max(spectrumbusiness) as spectrumbusiness,
                    max(specmobile) as specmoapp,
                    max(biller_type) as biller_type,
                    date_denver,
                    prod.aes_decrypt256(division) as division,
                    prod.aes_decrypt256(division_id) as division_id,
                    max(prod.aes_decrypt256(site_sys)) as sys,
                    max(prod.aes_decrypt256(prn)) as prn,
                    max(prod.aes_decrypt256(agn)) as agn,
                    max(prod.aes_decrypt256(acct_site_id)) as acct_site_id,
                    max(prod.aes_decrypt256(acct_company)) as acct_company,
                    max(prod.aes_decrypt256(acct_franchise)) as acct_franchise
             FROM asp_extract_login_data_daily_2months_pvt
            WHERE length(REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+','')) between 1 and 16
             group by date_denver,
                      REGEXP_REPLACE(PROD.AES_DECRYPT256(account_number),'^0+',''),
                      prod.aes_decrypt256(division),
                      prod.aes_decrypt256(division_id);
            " | sed 's/NULL//g' > $FILE_NAME.tsv

tar --force-local --warning=no-file-changed -czvf $FILE_NAME.tgz  $FILE_NAME.tsv
