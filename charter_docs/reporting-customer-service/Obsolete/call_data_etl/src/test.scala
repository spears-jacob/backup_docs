import org.apache.spark.sql.expressions._
import com.spectrum.crypto._
import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoders, Row, SaveMode }
import java.sql._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.temporal.ChronoUnit

//Setting case classes to be used in the process
case class CallCareAtom(account_id_encrypted: String, account_number_encrypted: Option[String], answered_disposition_description: String, answered_disposition_id: String, call_cause_description: Option[String], call_handled_flag: Boolean, call_inbound_key: String, call_issue_description: Option[String], call_notes_text: Option[String], call_owner_name: Option[String], call_resolution_description: Option[String], call_id: String, call_segment_number: String, call_type_code: Option[String], care_center_management_name: String, legacy_corp_code: String, created_by: Option[String], created_on_date: String, customer_account_number_encrypted: String, customer_call_center_flag: Option[Boolean], edu_id: Option[String], effective_hire_date: Option[String], job_role_name: Option[String], location_name: String, last_handled_segmentation_flag: Boolean, last_updated_date_time_stamp: String, mso_agent_name: Option[String], product_lob_description: Option[String], call_segment_stop_date_time: String, call_segment_stop_date_time_in_utc: String, call_segment_start_date_time: String, call_segment_start_date_time_in_utc: String, split_sum_description: Option[String], service_call_tracker_id: Option[String], truck_roll_flag: Boolean, track_phone_number_encrypted: Option[String], unified_call_id: String, partition_date: String, call_end_date_utc: String)

case class AccountData(encrypted_legacy_account_number_256: String, partition_date_denver: String, customer_type: String)

case class CallData(
                       call_inbound_key: String
                       ,call_id: String
                       ,call_start_date_utc: String
                       ,call_start_time_utc: String
                       ,call_end_time_utc: String
                       ,call_start_datetime_utc: String
                       ,call_end_datetime_utc: String
                       ,call_start_timestamp_utc: Double
                       ,call_end_timestamp_utc: Double
                       ,previous_call_time_utc: Option[Double]
                       ,segment_id: String
                       ,segment_number: String
                       ,segment_status_disposition: String
                       ,segment_start_time_utc: String
                       ,segment_end_time_utc: String
                       ,segment_start_datetime_utc: String
                       ,segment_end_datetime_utc: String
                       ,segment_start_timestamp_utc: Double
                       ,segment_end_timestamp_utc: Double
                       ,segment_duration_seconds: Double
                       ,segment_duration_minutes: Double
                       ,segment_handled_flag: Boolean
                       ,customer_call_count_indicator: String
                       ,call_handled_flag: Boolean
                       ,call_owner: String
                       ,product: String
                       ,account_number: String
                       ,customer_account_number: String
                       ,customer_type: Option[String]
                       ,customer_subtype: Option[String]
                       ,truck_roll_flag: Boolean
                       ,notes_txt: String
                       ,resolution_description: String
                       ,cause_description: String
                       ,issue_description: String
                       ,company_code: String
                       ,service_call_tracker_id: String
                       ,created_on: String
                       ,created_by: String
                       ,phone_number_from_tracker: String
                       ,call_type: String
                       ,split_sum_desc: String
                       ,location_name: String
                       ,care_center_management_name: String
                       ,agent_job_role_name: String
                       ,agent_effective_hire_date: String
                       ,agent_mso: String
                       ,eduid: String
                       ,last_handled_segment_flag: Boolean
                       ,record_update_timestamp: String
                       ,source: String
                       ,enhanced_account_number:  Boolean
                       ,call_end_date_utc: String
                   )

//Setting up the encrypt and decrypt functions
val encryptString: (String => String) = Aes.encrypt(_)
val encryptStringUDF = udf(encryptString)
val encryptString256: (String => String) = Aes.encrypt256(_)
val encryptString256UDF = udf(encryptString256)

val decryptString: (String => String) = Aes.decrypt(_)
val decryptStringUDF = udf(decryptString)
val decryptString256: (String => String) = Aes.decrypt256(_)
val decryptString256UDF = udf(decryptString256)

val endDate = "2018-10-15"
val overwriteDate = endDate
val environment = "dev"

val callCareAtom = {
  spark.table(s"dev.steve_call_care_large") //STN change for prod testing, prod.atom_call_care, dev.steve_call_care_large
      .withColumn("call_id",trim($"caller_id"))
      .withColumn("call_segment_number",$"call_segmentation_number")
      .withColumn("call_segment_start_date_time_in_utc",
        to_utc_timestamp(substring($"call_segment_start_date_time_in_est",0,19),"America/New_York"))
      .withColumn("call_segment_stop_date_time_in_utc",
        to_utc_timestamp(substring($"call_segment_stop_date_time_in_est",0,19),"America/New_York"))
      .withColumn("call_end_date_utc", to_date($"call_segment_stop_date_time_in_est"))
      .where(to_date($"partition_date") === endDate)
      .as[CallCareAtom]
}.cache

val accountsForDate = {spark.table("prod.quantum_atom_snapshot_accounts_v")
    .withColumn("customer_type_extended", coalesce(upper($"customer_type"), $"extract_type"))
    .select(
      $"partition_date_denver",
      $"customer_type_extended".alias("customer_type"),
      $"encrypted_legacy_account_number_256")
    //,concat_ws("::", $"extract_source", $"extract_type", $"legacy_company", $"customer_type").as("customer_type")  DON'T KNOW IF THIS IS NEEDED
    .where($"partition_date_denver" >= date_sub(lit(overwriteDate), 1)
    && $"partition_date_denver" <= lit(endDate))
    .distinct
    .as[AccountData]}.cache

val venona_events_portals = (
    spark.table("prod.venona_events_portals")
        .select(
          $"visit__account__account_number"
          ,$"visit__visit_id".alias("visit_id")
          ,$"visit__application_details__application_name"
          ,$"message__category"
          ,$"message__name"
          // TODO: (ghmulli) we need to make sure to use this in newer data. need to figure out when this was added...
          //$"visit__account__account_billing_id",
          // TODO: (ghmulli) use message.timestamp ?
          ,$"received__timestamp"
          ,$"partition_date_utc"
        )
        .withColumn("visit_account_number",encryptStringUDF(regexp_replace(substring_index(decryptStringUDF($"visit__account__account_number"),"-",-2),"-","")))
        .where(
          $"partition_date_utc" >= date_sub(lit(overwriteDate), 2) &&
              $"partition_date_utc" <= lit(endDate)

        )
    ).as("portals").cache

val filtered_venona_events_portals = (
    venona_events_portals.where(
              $"visit_account_number".isNotNull &&
              $"visit_id".isNotNull &&
              $"visit_account_number" =!= "GSWNkZXIfDPD6x25Na3i8g==" &&
              $"visit_account_number" =!= "7FbKtybuOWU4/Q0SRInbHA=="
        )
        .withColumn("received__timestamp", ($"received__timestamp"/1000).cast("BIGINT"))
        .withColumn("visit_type", lower($"visit__application_details__application_name"))
        .distinct
    ).as("portals_filter")


  /*
   * calls_with_visit – the number of calls where customer visited a help portal (Spectrum.net/SB.net/MSA)
   *  and then called into CARE within 24 hours of that visit
   */



  /*
   * handled_acct_calls – the number of validated (call with account number) handled calls
   *  (handled calls means customer spoke with an agent)
   */
{callCareAtom
       .withColumn("segment_handled_flag", when($"call_handled_flag" === true && lower($"call_owner_name") === "customer operations" && lower($"answered_disposition_description") === "answered by an agent",1).otherwise(0))
         .where($"segment_handled_flag" === true
             && lower(decryptStringUDF($"account_number_encrypted")) =!= "unknown"
         && $"mso_agent_name" === "CHR")
       .select("call_end_date_utc", "call_inbound_key").distinct
       .orderBy("call_end_date_utc").groupBy("call_end_date_utc").count}.show(100, false)

  /*
   * total_acct_calls – number of distinct account numbers that called
   */
  {callCareAtom
          .where($"mso_agent_name" === "CHR"
          && lower(decryptStringUDF($"account_number_encrypted")) =!= "unknown")
      .select("call_end_date_utc", "account_number_encrypted").distinct
      .orderBy("call_end_date_utc").groupBy("call_end_date_utc").count}.show(100, false)

  /*
   * total_calls – number of handled calls (total)
   */
  {callCareAtom
        .select("call_end_date_utc", "call_inbound_key", "mso_agent_name")
        .orderBy("call_end_date_utc", "mso_agent_name")
        .groupBy("call_end_date_utc", "mso_agent_name")
        .count}.show(100, false)


  /*
   * total_acct_visits – number of authenticated visits to a help portal
   *  is a count of accounts that called instead of the count of calls
   */
  {filtered_venona_events_portals
          .join(
            callCareAtom,
            venona_events_portals("visit_account_number") === callCareAtom("account_number_encrypted"),
            "left"
          )
        .where($"partition_date_utc" >= lit(overwriteDate))
        .withColumn("care_events_customer_type",
          when($"visit_type" === "myspectrum" || $"visit_type" === "specnet", "RESIDENTIAL")
              .otherwise(when($"visit_type" === "smb", "COMMERCIAL").otherwise("UNMAPPED")))
      .orderBy("partition_date_utc", "mso_agent_name", "care_events_customer_type")
      .groupBy("partition_date_utc", "mso_agent_name", "care_events_customer_type").count}.show(100, false)

  /*
   * total_visits – total number of visits to a help portal
   */
  {venona_events_portals.select("visit_id").distinct.count}

(venona_events_portals
    .where($"visit_account_number".isNotNull
        && $"visit_id".isNotNull)
    .withColumn("care_events_partition_date_utc",$"partition_date_utc")
    .withColumn("care_events_customer_type",upper($"portals_join_customer_type"))
    .withColumn("care_events_visit_type",upper($"visit_type"))
    .groupBy($"care_events_partition_date_utc")
    .agg(countDistinct($"visit_account_number").alias("total_acct_visits")
      ,countDistinct($"visit_id").alias("care_events_total_visits")
    )
).as("cs_visit_rate_care")

































