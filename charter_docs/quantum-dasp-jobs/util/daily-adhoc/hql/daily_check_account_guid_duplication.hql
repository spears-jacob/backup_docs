-- The following SET statements are needed to run this in hive.
-- SET sethive.strict.checks.cartesian.product=false;
-- SET hive.mapred.mode=notstrict;

INSERT INTO stg_dasp.asp_duplicate_account_guids_vs_bck
SELECT current_date AS date_of_run,
       COUNT(DISTINCT billing_combo_key) AS ct_dup_guids,
       ct_bck,
       ROUND(cast(COUNT(distinct billing_combo_key) AS double)/cast(ct_bck AS double),5) AS proportion_duplicated
FROM( SELECT   billing_combo_key,
               COUNT(encrypted_account_guid_256) AS ct_ac_guid
      FROM     prod.atom_identity_lookup
      GROUP BY billing_combo_key
      HAVING COUNT(encrypted_account_guid_256) > 1) inner_query
INNER JOIN (SELECT current_date AS cd, COUNT(DISTINCT billing_combo_key) AS ct_bck FROM prod.atom_identity_lookup) fullbck ON current_date = cd
GROUP BY ct_bck
;
