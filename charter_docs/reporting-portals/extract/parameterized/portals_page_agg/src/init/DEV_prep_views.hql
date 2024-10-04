USE prod;
--TABLEAU VIEWS
DROP VIEW asp_v_page_pathing_counts;
DROP VIEW asp_v_idm_page_set_pathing_agg;
DROP VIEW asp_v_msa_page_set_pathing_agg;
DROP VIEW asp_v_smb_page_set_pathing_agg;
DROP VIEW asp_v_spec_page_set_pathing_agg;

CREATE VIEW if not exists asp_v_page_pathing_counts AS
SELECT
     denver_date,
     unit_type,
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     modal_view_count,
     page_view_count,
     select_action_count,
     spinner_success_count,
     spinner_failure_count,
     toggle_flips_count
  FROM dev.asp_page_agg_counts
 WHERE denver_date            >= DATE_SUB(CURRENT_DATE,90)
;

CREATE VIEW if not exists asp_v_idm_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM dev.asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         =  'IDManagement'
;

CREATE VIEW if not exists asp_v_msa_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM dev.asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         = 'MySpectrum'
;

CREATE VIEW if not exists asp_v_smb_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM dev.asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         = 'SMB'
;

CREATE VIEW if not exists asp_v_spec_page_set_pathing_agg AS
SELECT
     current_page_name,
     previous_page_name,
     mso,
     'SpecNet'                  AS application_name,
     select_action_types_value,
     grouping_id,
     metric_name,
     metric_value,
     denver_date
  FROM dev.asp_page_set_pathing_agg
 WHERE denver_date              >= DATE_SUB(CURRENT_DATE,90)
   and application_name         IN ('specnet', 'SpecNet')
;
