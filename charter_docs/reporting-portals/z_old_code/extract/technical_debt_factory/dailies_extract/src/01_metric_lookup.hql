set hive.vectorized.execution.enabled = false;
USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------
-------------------- ***** Create Resi Lookup Table ***** ----------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:LKP_db}.asp_pm_metric_lkp PURGE;
CREATE TABLE IF NOT EXISTS ${env:LKP_db}.asp_pm_metric_lkp
(
  report_suite STRING,
  company STRING,
  portal STRING,
  hive_metric STRING,
  tableau_name STRING,
  quantum_start_fm STRING,
  metric_family STRING,
  associated_attempt_metric STRING
)
;

INSERT INTO TABLE ${env:LKP_db}.asp_pm_metric_lkp VALUES
('resi','CHTR','Spectrum.net','password_reset_attempts','Attempts to Reset Password',NULL,'Identity','NULL'),
('resi','TWC','TimeWarnercable.com','password_reset_attempts','Attempts to Reset Password',NULL,'Identity','NULL'),
('resi','BHN','BrightHouse.com','password_reset_attempts','Attempts to Reset Password',NULL,'Identity','NULL'),
('resi','CHTR','Spectrum.net','canceled_appointments','Cancelled Service Appointments','2018-09','Appointment Mgmt.','NULL'),
('resi','TWC','TimeWarnercable.com','canceled_appointments','Cancelled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('resi','BHN','BrightHouse.com','canceled_appointments','Cancelled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('resi','CHTR','Spectrum.net','modem_router_resets','Modem Router Resets','2018-09','Equip. Troubleshooting','NULL'),
('resi','TWC','TimeWarnercable.com','modem_router_resets','Modem Router Resets',NULL,'Equip. Troubleshooting','NULL'),
('resi','BHN','BrightHouse.com','modem_router_resets','Modem Router Resets',NULL,'Equip. Troubleshooting','NULL'),
('resi','CHTR','Spectrum.net','one_time_payments','One-Time Payments (OTP)','2018-09','Billing Transactions','NULL'),
('resi','TWC','TimeWarnercable.com','one_time_payments','One-Time Payments (OTP)',NULL,'Billing Transactions','NULL'),
('resi','BHN','BrightHouse.com','one_time_payments','One-Time Payments (OTP)',NULL,'Billing Transactions','NULL'),
('resi','CHTR','Spectrum.net','refresh_digital_receiver_requests','Refresh Digital Receivers Requests','2018-09','Equip. Troubleshooting','NULL'),
('resi','TWC','TimeWarnercable.com','refresh_digital_receiver_requests','Refresh Digital Receivers Requests',NULL,'Equip. Troubleshooting','NULL'),
('resi','BHN','BrightHouse.com','refresh_digital_receiver_requests','Refresh Digital Receivers Requests',NULL,'Equip. Troubleshooting','NULL'),
('resi','CHTR','Spectrum.net','rescheduled_appointments','Rescheduled Service Appointments','2018-09','Appointment Mgmt.','NULL'),
('resi','TWC','TimeWarnercable.com','rescheduled_appointments','Rescheduled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('resi','BHN','BrightHouse.com','rescheduled_appointments','Rescheduled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('resi','CHTR','Spectrum.net','web_sessions_visits','Sessions / Visits','2018-09','Overall Metrics','NULL'),
('resi','TWC','TimeWarnercable.com','web_sessions_visits','Sessions / Visits',NULL,'Overall Metrics','NULL'),
('resi','BHN','BrightHouse.com','web_sessions_visits','Sessions / Visits',NULL,'Overall Metrics','NULL'),
('resi','CHTR','Spectrum.net','set_up_auto_payments','Set-up Auto-Payments (AP)','2018-09','Billing Transactions','NULL'),
('resi','TWC','TimeWarnercable.com','set_up_auto_payments','Set-up Auto-Payments (AP)',NULL,'Billing Transactions','NULL'),
('resi','BHN','BrightHouse.com','set_up_auto_payments','Set-up Auto-Payments (AP)',NULL,'Billing Transactions','NULL'),
('resi','CHTR','Spectrum.net','password_reset_success','Successful Password Resets',NULL,'Identity','password_reset_attempts'),
('resi','TWC','TimeWarnercable.com','password_reset_success','Successful Password Resets',NULL,'Identity','password_reset_attempts'),
('resi','BHN','BrightHouse.com','password_reset_success','Successful Password Resets',NULL,'Identity','password_reset_attempts'),
('resi','CHTR','Spectrum.net','support_section_page_views','Support Section Page Views','2018-09','Support','NULL'),
('resi','TWC','TimeWarnercable.com','support_section_page_views','Support Section Page Views',NULL,'Support','NULL'),
('resi','BHN','BrightHouse.com','support_section_page_views','Support Section Page Views',NULL,'Support','NULL'),
('resi','CHTR','Spectrum.net','id_recovery_attempts','Total Attempts to Recover ID',NULL,'Identity','NULL'),
('resi','TWC','TimeWarnercable.com','id_recovery_attempts','Total Attempts to Recover ID',NULL,'Identity','NULL'),
('resi','BHN','BrightHouse.com','id_recovery_attempts','Total Attempts to Recover ID',NULL,'Identity','NULL'),
('resi','CHTR','Spectrum.net','id_creation_attempts','Total New IDs Create Attempts',NULL,'Identity','NULL'),
('resi','TWC','TimeWarnercable.com','id_creation_attempts','Total New IDs Create Attempts',NULL,'Identity','NULL'),
('resi','BHN','BrightHouse.com','id_creation_attempts','Total New IDs Create Attempts',NULL,'Identity','NULL'),
('resi','CHTR','Spectrum.net','ids_created','Total New IDs Created',NULL,'Identity','id_creation_attempts'),
('resi','TWC','TimeWarnercable.com','ids_created','Total New IDs Created',NULL,'Identity','id_creation_attempts'),
('resi','BHN','BrightHouse.com','ids_created','Total New IDs Created',NULL,'Identity','id_creation_attempts'),
('resi','CHTR','Spectrum.net','id_recovery_success','Total Successful ID Recovery',NULL,'Identity','id_recovery_attempts'),
('resi','TWC','TimeWarnercable.com','id_recovery_success','Total Successful ID Recovery',NULL,'Identity','id_recovery_attempts'),
('resi','BHN','BrightHouse.com','id_recovery_success','Total Successful ID Recovery',NULL,'Identity','id_recovery_attempts'),
('resi','CHTR','Spectrum.net','hhs_logged_in','Unique Households [Authenticated]',NULL,'Overall Metrics','NULL'),
('resi','TWC','TimeWarnercable.com','hhs_logged_in','Unique Households [Authenticated]',NULL,'Overall Metrics','NULL'),
('resi','BHN','BrightHouse.com','hhs_logged_in','Unique Households [Authenticated]',NULL,'Overall Metrics','NULL'),
('resi','CHTR','Spectrum.net','view_statements','View Online Statement','2018-09','Billing Transactions','NULL'),
('resi','TWC','TimeWarnercable.com','view_statements','View Online Statement',NULL,'Billing Transactions','NULL'),
('resi','BHN','BrightHouse.com','view_statements','View Online Statement',NULL,'Billing Transactions','NULL'),
('app','CHTR','My Spectrum','view_statements','View Online Statements','2018-08','Billing Transactions','NULL'),
('app','TWC','My Spectrum','view_statements','View Online Statements','2018-08','Billing Transactions','NULL'),
('app','BHN','My Spectrum','view_statements','View Online Statements','2018-08','Billing Transactions','NULL'),
('app','MyTWC','MyTWC','view_statements','View Online Statements',NULL,'Billing Transactions','NULL'),
('app','My Spectrum','My Spectrum','view_statements','View Online Statements',NULL,'Billing Transactions','NULL'),
('app','CHTR','My Spectrum','unique_visitors','Unique Visitors','2018-08','Overall Metrics','NULL'),
('app','TWC','My Spectrum','unique_visitors','Unique Visitors','2018-08','Overall Metrics','NULL'),
('app','BHN','My Spectrum','unique_visitors','Unique Visitors','2018-08','Overall Metrics','NULL'),
('app','MyTWC','MyTWC','unique_visitors','Unique Visitors',NULL,'Overall Metrics','NULL'),
('app','My Spectrum','My Spectrum','unique_visitors','Unique Visitors',NULL,'Overall Metrics','NULL'),
('app','CHTR','My Spectrum','support_section_page_views','Support Section','2018-08','Support','NULL'),
('app','TWC','My Spectrum','support_section_page_views','Support Section','2018-08','Support','NULL'),
('app','BHN','My Spectrum','support_section_page_views','Support Section','2018-08','Support','NULL'),
('app','MyTWC','MyTWC','support_section_page_views','Support Section',NULL,'Support','NULL'),
('app','My Spectrum','My Spectrum','support_section_page_views','Support Section',NULL,'Support','NULL'),
('app','CHTR','My Spectrum','set_up_auto_payments','Set-Up AutoPay','2018-08','Billing Transactions','NULL'),
('app','TWC','My Spectrum','set_up_auto_payments','Set-Up AutoPay','2018-08','Billing Transactions','NULL'),
('app','BHN','My Spectrum','set_up_auto_payments','Set-Up AutoPay','2018-08','Billing Transactions','NULL'),
('app','MyTWC','MyTWC','set_up_auto_payments','Set-Up AutoPay',NULL,'Billing Transactions','NULL'),
('app','My Spectrum','My Spectrum','set_up_auto_payments','Set-Up AutoPay',NULL,'Billing Transactions','NULL'),
('app','CHTR','My Spectrum','rescheduled_appointments','Rescheduled Service Appointments','2018-08','Appointment Mgmt.','NULL'),
('app','TWC','My Spectrum','rescheduled_appointments','Rescheduled Service Appointments','2018-08','Appointment Mgmt.','NULL'),
('app','BHN','My Spectrum','rescheduled_appointments','Rescheduled Service Appointments','2018-08','Appointment Mgmt.','NULL'),
('app','MyTWC','MyTWC','rescheduled_appointments','Rescheduled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('app','My Spectrum','My Spectrum','rescheduled_appointments','Rescheduled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('app','CHTR','My Spectrum','refresh_digital_receiver_requests','Refresh Digital Receiver Requests','2018-08','Equip. Troubleshooting','NULL'),
('app','TWC','My Spectrum','refresh_digital_receiver_requests','Refresh Digital Receiver Requests','2018-08','Equip. Troubleshooting','NULL'),
('app','BHN','My Spectrum','refresh_digital_receiver_requests','Refresh Digital Receiver Requests','2018-08','Equip. Troubleshooting','NULL'),
('app','MyTWC','MyTWC','refresh_digital_receiver_requests','Refresh Digital Receiver Requests',NULL,'Equip. Troubleshooting','NULL'),
('app','My Spectrum','My Spectrum','refresh_digital_receiver_requests','Refresh Digital Receiver Requests',NULL,'Equip. Troubleshooting','NULL'),
('app','CHTR','My Spectrum','one_time_payments','One Time Payment','2018-08','Billing Transactions','NULL'),
('app','TWC','My Spectrum','one_time_payments','One Time Payment','2018-08','Billing Transactions','NULL'),
('app','BHN','My Spectrum','one_time_payments','One Time Payment','2018-08','Billing Transactions','NULL'),
('app','MyTWC','MyTWC','one_time_payments','One Time Payment',NULL,'Billing Transactions','NULL'),
('app','My Spectrum','My Spectrum','one_time_payments','One Time Payment',NULL,'Billing Transactions','NULL'),
('app','CHTR','My Spectrum','modem_router_resets','Modem Router Resets','2018-08','Equip. Troubleshooting','NULL'),
('app','TWC','My Spectrum','modem_router_resets','Modem Router Resets','2018-08','Equip. Troubleshooting','NULL'),
('app','BHN','My Spectrum','modem_router_resets','Modem Router Resets','2018-08','Equip. Troubleshooting','NULL'),
('app','MyTWC','MyTWC','modem_router_resets','Modem Router Resets',NULL,'Equip. Troubleshooting','NULL'),
('app','My Spectrum','My Spectrum','modem_router_resets','Modem Router Resets',NULL,'Equip. Troubleshooting','NULL'),
('app','My Spectrum','My Spectrum','id_recovery_success','Forgot Username Success','2018-08','Identity','NULL'),
('app','MyTWC','MyTWC','id_recovery_success','Forgot Username Success','2018-08','Identity','NULL'),
('app','TWC','My Spectrum','id_recovery_success','Forgot Username Success','2018-08','Identity','NULL'),
('app','BHN','My Spectrum','id_recovery_success','Forgot Username Success','2018-08','Identity','NULL'),
('app','CHTR','My Spectrum','id_recovery_success','Forgot Username Success','2018-08','Identity','NULL'),
('app','My Spectrum','My Spectrum','password_reset_success','Forgot Password Success','2018-08','Identity','NULL'),
('app','MyTWC','MyTWC','password_reset_success','Forgot Password Success','2018-08','Identity','NULL'),
('app','TWC','My Spectrum','password_reset_success','Forgot Password Success','2018-08','Identity','NULL'),
('app','BHN','My Spectrum','password_reset_success','Forgot Password Success','2018-08','Identity','NULL'),
('app','CHTR','My Spectrum','password_reset_success','Forgot Password Success','2018-08','Identity','NULL'),
('app','CHTR','My Spectrum','canceled_appointments','Cancelled Appointments','2018-08','Appointment Mgmt.','NULL'),
('app','TWC','My Spectrum','canceled_appointments','Cancelled Appointments','2018-08','Appointment Mgmt.','NULL'),
('app','BHN','My Spectrum','canceled_appointments','Cancelled Appointments','2018-08','Appointment Mgmt.','NULL'),
('app','MyTWC','MyTWC','canceled_appointments','Cancelled Appointments',NULL,'Appointment Mgmt.','NULL'),
('app','My Spectrum','My Spectrum','canceled_appointments','Cancelled Appointments',NULL,'Appointment Mgmt.','NULL'),
('app','CHTR','My Spectrum','call_support_or_request_callback','Call Support or Request Callback','2018-08','Customer Service Contacts','NULL'),
('app','TWC','My Spectrum','call_support_or_request_callback','Call Support or Request Callback','2018-08','Customer Service Contacts','NULL'),
('app','BHN','My Spectrum','call_support_or_request_callback','Call Support or Request Callback','2018-08','Customer Service Contacts','NULL'),
('app','MyTWC','MyTWC','call_support_or_request_callback','Call Support or Request Callback',NULL,'Customer Service Contacts','NULL'),
('app','My Spectrum','My Spectrum','call_support_or_request_callback','Call Support or Request Callback',NULL,'Customer Service Contacts','NULL'),
('app','CHTR','My Spectrum','web_sessions_visits','Authenticated Sessions / Visits','2018-08','Overall Metrics','NULL'),
('app','TWC','My Spectrum','web_sessions_visits','Authenticated Sessions / Visits','2018-08','Overall Metrics','NULL'),
('app','BHN','My Spectrum','web_sessions_visits','Authenticated Sessions / Visits','2018-08','Overall Metrics','NULL'),
('app','MyTWC','MyTWC','web_sessions_visits','Authenticated Sessions / Visits',NULL,'Overall Metrics','NULL'),
('app','My Spectrum','My Spectrum','web_sessions_visits','Authenticated Sessions / Visits',NULL,'Overall Metrics','NULL'),
('app','TWC','My Spectrum','app_downloads','App Downloads',NULL,'Overall Metrics','NULL'),
('app','BHN','My Spectrum','app_downloads','App Downloads',NULL,'Overall Metrics','NULL'),
('app','MyTWC','MyTWC','app_downloads','App Downloads',NULL,'Overall Metrics','NULL'),
('app','My Spectrum','My Spectrum','app_downloads','App Downloads',NULL,'Overall Metrics','NULL'),
('smb','CHTR','SpectrumBusiness.net','id_recovery_attempts','Attempts to Recover ID','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','id_recovery_attempts','Attempts to Recover ID',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','id_recovery_attempts','Attempts to Recover ID',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','password_reset_attempts','Attempts to Reset Password','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','password_reset_attempts','Attempts to Reset Password',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','password_reset_attempts','Attempts to Reset Password',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','set_up_auto_payments','Auto Pay Setup(AP) Successes','2018-09','Billing Transactions','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','set_up_auto_payments','Auto Pay Setup(AP) Successes',NULL,'Billing Transactions','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','set_up_auto_payments','Auto Pay Setup(AP) Successes',NULL,'Billing Transactions','NULL'),
('smb','CHTR','SpectrumBusiness.net','canceled_appointments','Cancelled Service Appointments','2018-09','Appointment Mgmt.','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','canceled_appointments','Cancelled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','canceled_appointments','Cancelled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('smb','CHTR','SpectrumBusiness.net','new_sub_users_created','New Sub Users Created','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','new_sub_users_created','New Sub Users Created',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','new_sub_users_created','New Sub Users Created',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','one_time_payments','One Time Payments(OTP)','2018-09','Billing Transactions','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','one_time_payments','One Time Payments(OTP)',NULL,'Billing Transactions','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','one_time_payments','One Time Payments(OTP)',NULL,'Billing Transactions','NULL'),
('smb','CHTR','SpectrumBusiness.net','view_statements','Online Statement Views','2018-09','Billing Transactions','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','view_statements','Online Statement Views',NULL,'Billing Transactions','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','view_statements','Online Statement Views',NULL,'Billing Transactions','NULL'),
('smb','CHTR','SpectrumBusiness.net','rescheduled_appointments','Rescheduled Service Appointments','2018-09','Appointment Mgmt.','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','rescheduled_appointments','Rescheduled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','rescheduled_appointments','Rescheduled Service Appointments',NULL,'Appointment Mgmt.','NULL'),
('smb','CHTR','SpectrumBusiness.net','web_sessions_visits','Sessions / Visits','2018-09','Overall Metrics','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','web_sessions_visits','Sessions / Visits',NULL,'Overall Metrics','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','web_sessions_visits','Sessions / Visits',NULL,'Overall Metrics','NULL'),
('smb','CHTR','SpectrumBusiness.net','support_section_page_views','Support Section Page Views','2018-09','Support','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','support_section_page_views','Support Section Page Views',NULL,'Support','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','support_section_page_views','Support Section Page Views',NULL,'Support','NULL'),
('smb','CHTR','SpectrumBusiness.net','id_creation_attempts','Total New IDs Create Attempts','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','id_creation_attempts','Total New IDs Create Attempts',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','id_creation_attempts','Total New IDs Create Attempts',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','ids_created','Total New IDs Created','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','ids_created','Total New IDs Created',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','ids_created','Total New IDs Created',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','new_sub_user_creation_attempts','Total Sub User Creation Attempts','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','new_sub_user_creation_attempts','Total Sub User Creation Attempts',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','new_sub_user_creation_attempts','Total Sub User Creation Attempts',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','password_reset_success','Total Successful Password Resets','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','password_reset_success','Total Successful Password Resets',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','password_reset_success','Total Successful Password Resets',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','id_recovery_success','Total Username Recovery Successes','2018-09','Identity','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','id_recovery_success','Total Username Recovery Successes',NULL,'Identity','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','id_recovery_success','Total Username Recovery Successes',NULL,'Identity','NULL'),
('smb','CHTR','SpectrumBusiness.net','hhs_logged_in','Unique Subscribers(BIZ) Logged In',NULL,'Overall Metrics','NULL'),
('smb','TWC','Business.TimeWarnerCable.com','hhs_logged_in','Unique Subscribers(BIZ) Logged In',NULL,'Overall Metrics','NULL'),
('smb','BHN','BusinessAccount.Brighthouse.com','hhs_logged_in','Unique Subscribers(BIZ) Logged In',NULL,'Overall Metrics','NULL')
;

SELECT '
--------------------------------------------------------------------------------
----------------- ***** END: Create Metric Lookup Table ***** ------------------
--------------------------------------------------------------------------------

';
