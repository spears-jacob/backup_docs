USE ${env:ENVIRONMENT};

SELECT
      CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
        THEN PROD.AES_DECRYPT256(account_number)
        ELSE PROD.AES_DECRYPT(account_number) END
        as account_number,
       myspectrum,
       specnet,
       spectrumbusiness,
       date_denver,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
         THEN prod.aes_decrypt256(division_id)
         ELSE division_id END
         as division_id,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
           THEN prod.aes_decrypt256(site_sys)
           ELSE site_sys END
           as site_sys,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
           THEN prod.aes_decrypt256(prn)
           ELSE prn END
           as prn,
       CASE WHEN PROD.AES_DECRYPT256(account_number) IS NOT NULL
             THEN prod.aes_decrypt256(agn)
             ELSE agn END
             as agn
FROM ${hiveconf:table_name}
WHERE date_denver >= '${hiveconf:extract_start_date}'
AND date_denver <= '${hiveconf:extract_end_date}'
;
