import org.apache.spark.sql.expressions._
import com.spectrum.crypto._
import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoders, Row, SaveMode }
import java.sql._
import java.time.LocalDate
import java.time.temporal.ChronoUnit

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

//val overwritedate = sys.env("overwritedate")
//val startdate = sys.env("overwritedate")
//val enddate = sys.env("enddate")
val environment = "dev" //sys.env("ENVIRONMENT")
val tmp_db = "tmp_dev" //sys.env("TMP_db")

//Development/Debugging settings
// val environment = "prod"
// val tmp_db = sys.env("TMP_db")
val overwritedate = "2018-10-19"
val startdate = "2018-10-19"
val enddate = "2018-10-26"


//Setting up the encrypt and decrypt functions
val encryptString: (String => String) = Aes.encrypt(_)
val encryptStringUDF = udf(encryptString)
val encryptString256: (String => String) = Aes.encrypt256(_)
val encryptString256UDF = udf(encryptString256)

val decryptString: (String => String) = Aes.decrypt(_)
val decryptStringUDF = udf(decryptString)
val decryptString256: (String => String) = Aes.decrypt256(_)
val decryptString256UDF = udf(decryptString256)

def daysBetween(from: LocalDate, to: LocalDate): Seq[LocalDate] = (
    Iterator
        .iterate(from)(_.plusDays(1))
        .take(ChronoUnit.DAYS.between(from, to.plusDays(1)).toInt)
        .toSeq
    )

try {

    //Step 1: Get Quantum data for Spectrum.net, SB.net, and MySpectrum App for the new timeframe
    val venona_events_portals = (
      spark.table("prod.venona_events_portals")
      .select(
        $"visit__account__account_number"
        ,$"visit__visit_id"
        ,$"visit__application_details__application_name"
        ,$"message__category"
        ,$"message__name"
        ,$"received__timestamp"
        ,$"partition_date_utc"
      )
      .where(
        $"partition_date_utc" >= date_add(lit(startdate),-2).cast("date") &&
        $"partition_date_utc" <= to_date(lit(enddate))
        )
    )

    val account_data =
    {spark.table("prod.quantum_atom_snapshot_accounts_v")
          .select(
            $"account_key",
            $"customer_type",
            decryptString256UDF($"encrypted_legacy_account_number_256").alias("account_number"),
            $"extract_type"
          )
          .where($"partition_date_denver" === enddate && $"account_status" === "CONNECTED")
          .distinct
    }

      /*(
      spark.table("prod.account_current")
      .withColumn("account_number",decryptString256UDF($"account__number_aes256"))
      .withColumn("customer_type",when(isnull($"customer__type"),"UNMAPPED").otherwise($"customer__type"))
      .select($"account_number",$"customer_type")
      .distinct
    )*/

    val portal_event_data = (
      venona_events_portals
      .withColumn("received__timestamp", ($"received__timestamp"/1000).cast("BIGINT"))
      .withColumn("visit_type", lower($"visit__application_details__application_name"))
      .withColumn("decrypted_account", decryptStringUDF($"visit__account__account_number"))
      .join(account_data,$"decrypted_account" === $"account_number","left")
      .withColumn("customer_type",upper(coalesce($"extract_type",lit("UNMAPPED"))))
      .select(
        $"visit__account__account_number".alias("visit_account_number")
        ,$"visit__visit_id".alias("visit_id")
        ,$"customer_type"
        ,$"message__category"
        ,$"message__name"
        ,$"received__timestamp"
        ,$"partition_date_utc"
        ,$"visit_type"
      )
      .where(
          !isnull($"visit__account__account_number") &&
          !isnull($"visit__visit_id") &&
          $"visit__account__account_number" =!= "GSWNkZXIfDPD6x25Na3i8g==" &&
          $"visit__account__account_number" =!= "7FbKtybuOWU4/Q0SRInbHA=="
        )
      .distinct
    )

    //Step 2: Add new events into current events table which is used for reporting
    val steve_care_events = (
      portal_event_data
        //.withColumn("account_number_new", substring(decryptStringUDF($"account_number"),decryptStringUDF($"account_number").indexOf("-")))
        .withColumn("visit_account_number",encryptStringUDF(regexp_replace(substring_index(decryptStringUDF($"visit_account_number"),"-",-2),"-","")))
        .select(
        $"visit_account_number"
        ,$"visit_id"
        ,$"customer_type".alias("visit_customer_type")
        ,$"message__category"
        ,$"message__name"
        ,$"received__timestamp"
        ,$"visit_type"
        ,$"partition_date_utc"
      )
    )

    // //Step 3: Combine call AND event at page level and filter for calls with visits 24 hours prior
    val cs_calls_and_events = (
      steve_care_events
      .withColumn("events_account_number",decryptStringUDF($"visit_account_number"))
      .join(
        spark.table(s"${environment}.steve_call_data_enhanced")
        .withColumn("call_account_number",decryptString256UDF($"account_number"))
        .where($"call_end_date_utc" >= lit(startdate) &&
            $"call_end_date_utc" <= lit(enddate) &&
          $"enhanced_account_number" === lit(false))
        ,$"events_account_number" === $"call_account_number"
        ,"inner"
      )
      .where(
        abs(($"call_start_timestamp_utc"/1000) - $"received__timestamp") <= "86400")
      .select(
        $"visit_account_number"
        ,$"visit_id"
        ,$"visit_customer_type"
        ,$"message__category"
        ,$"message__name"
        ,$"received__timestamp"
        ,$"partition_date_utc"
        ,$"visit_type"
        ,$"agent_effective_hire_date"
        ,$"agent_job_role_name"
        ,$"agent_mso"
        ,$"account_number"
        ,$"call_end_date_utc"
        ,$"call_end_datetime_utc"
        ,$"call_end_time_utc"
        ,$"call_end_timestamp_utc"
        ,$"call_id"
        ,$"call_inbound_key"
        ,$"call_start_date_utc"
        ,$"call_start_datetime_utc"
        ,$"call_start_time_utc"
        ,$"call_start_timestamp_utc"
        ,$"call_type"
        ,$"care_center_management_name"
        ,$"cause_description"
        ,$"company_code"
        ,$"created_by"
        ,$"created_on"
        ,$"customer_account_number"
        ,$"segment_handled_flag"
        ,$"customer_subtype"
        ,$"customer_type"
        ,$"eduid"
        ,$"issue_description"
        ,$"last_handled_segment_flag"
        ,$"location_name"
        ,$"notes_txt"
        ,$"phone_number_from_tracker"
        ,$"previous_call_time_utc"
        ,$"product"
        ,$"record_update_timestamp"
        ,$"resolution_description"
        ,$"segment_duration_minutes"
        ,$"segment_duration_seconds"
        ,$"segment_end_datetime_utc"
        ,$"segment_end_time_utc"
        ,$"segment_end_timestamp_utc"
        ,$"segment_number"
        ,$"segment_start_datetime_utc"
        ,$"segment_start_time_utc"
        ,$"segment_start_timestamp_utc"
        ,$"service_call_tracker_id"
        ,$"split_sum_desc"
        ,$"truck_roll_flag"
      )
    )

    // Step 4: Create look up table for visit and call within 2 days interval, at visit level and limit to handled calls
    val cs_issue_cause_lookup = (
      spark.table(s"${environment}.steve_issue_cause_lookup")
        .withColumn("icl_issue_description", $"issue_description")
        .withColumn("icl_cause_description", $"cause_description")
        .withColumn("icl_call_group", $"call_group")
        .select(
           $"issue_category"
           ,$"cause_category"
           ,$"icl_issue_description"
           ,$"icl_cause_description"
           ,$"icl_call_group"
        )
    )

    val cs_resolution_lookup = (
      spark.table(s"${environment}.steve_resolution_lookup")
        .withColumn("l_resolution_description", $"resolution_description")
        .select(
          $"resolution_category"
          ,$"resolution_type"
          ,$"l_resolution_description"
       )
    )

    val steve_calls_with_prior_visit = (
      cs_calls_and_events
      .where($"segment_handled_flag" === lit(true))
      .join(cs_issue_cause_lookup, upper($"issue_description") === $"icl_issue_description" && upper($"cause_description") === $"icl_cause_description" && upper($"call_type") === $"icl_call_group", "left")
      .join(cs_resolution_lookup, upper($"resolution_description") === $"l_resolution_description", "left")
      .groupBy(
        $"account_number"
        ,$"visit_type"
        ,$"call_inbound_key"
        ,$"customer_type"
        ,$"customer_subtype"
        ,$"agent_mso"
        ,$"product"
        ,$"issue_description"
        ,$"cause_description"
        ,$"resolution_description"
        ,$"issue_category"
        ,$"cause_category"
        ,$"resolution_category"
        ,$"resolution_type"
        ,$"call_start_timestamp_utc"
        ,$"call_end_date_utc"
        ,$"previous_call_time_utc"
      )
      .agg(min($"received__timestamp").alias("visitStart"), max($"received__timestamp").alias("visitEnd"))
      .where($"visitStart" < ($"call_start_timestamp_utc" / 1000) && ($"visitStart" > ($"previous_call_time_utc" / 1000) || isnull($"previous_call_time_utc")) )
      .withColumn("call_start_div", ($"call_start_timestamp_utc"/1000) )
      .select(
        $"account_number"
        ,$"call_inbound_key"
        ,$"customer_type"
        ,$"customer_subtype"
        ,$"agent_mso"
        ,$"product"
        ,$"issue_description"
        ,$"cause_description"
        ,$"resolution_description"
        ,$"issue_category"
        ,$"cause_category"
        ,$"resolution_category"
        ,$"resolution_type"
        ,$"call_start_div"
        ,$"visit_type"
        ,$"visitStart"
        ,$"call_end_date_utc"
      )
    )

    //Load table that were formerly call_in_rate views
    val cs_visit_rate_steve_care_events = (
      steve_care_events
        .where(!isnull($"visit_account_number") && !isnull($"visit_id") &&
          $"partition_date_utc" >= startdate && $"partition_date_utc" <= enddate
        )
        .withColumn("care_events_partition_date_utc",$"partition_date_utc")
        .withColumn("care_events_customer_type",upper($"visit_customer_type"))
        .withColumn("care_events_visit_type",upper($"visit_type"))
        .groupBy($"care_events_partition_date_utc",$"care_events_customer_type",$"care_events_visit_type")
        .agg(countDistinct($"visit_account_number").alias("total_acct_visits")
          ,countDistinct($"visit_id").alias("care_events_total_visits")
      )
    )

    val cs_visit_rate_call_data = (
      spark.table(s"${environment}.steve_call_data_enhanced")
        .withColumn("call_data_call_date",$"call_end_date_utc")
        .withColumn("call_data_agent_mso",upper($"agent_mso"))
        .withColumn("call_data_customer_type",upper($"customer_type"))
        .where($"segment_handled_flag" === lit(true) &&
          $"call_end_date_utc" >= startdate && $"call_end_date_utc" <= enddate
        )
        .groupBy($"call_data_call_date",$"call_data_agent_mso",$"call_data_customer_type")
        .agg(countDistinct(when($"enhanced_account_number" === lit(false),$"account_number")).alias("call_data_total_acct_calls")
          ,countDistinct($"call_inbound_key").alias("call_data_total_calls")
          ,countDistinct(when(lower(decryptString256UDF($"account_number")) =!= "unknown" && $"enhanced_account_number" === lit(false), $"call_inbound_key")).alias("call_data_handled_acct_calls")
      )
    )

    val cs_visit_calls_with_prior_visit = (
      steve_calls_with_prior_visit
        .where($"call_end_date_utc" >= startdate && $"call_end_date_utc" <= enddate)
        .withColumn("prior_visit_call_date",$"call_end_date_utc")
        .withColumn("prior_visit_visit_type",upper($"visit_type"))
        .withColumn("prior_visit_agent_mso",upper($"agent_mso"))
        .withColumn("prior_visit_customer_type",upper($"customer_type"))
        .groupBy($"prior_visit_call_date",$"prior_visit_agent_mso",$"prior_visit_customer_type",$"prior_visit_visit_type")
        .agg(countDistinct($"call_inbound_key").alias("prior_visit_calls_with_visit")
      )
    )

    val steve_visit_rate_4calls = (
      cs_visit_rate_steve_care_events
        .join(cs_visit_rate_call_data,
          $"care_events_partition_date_utc" === $"call_data_call_date"
          && upper($"care_events_customer_type") === upper($"call_data_customer_type")
          ,"outer")
        .join(cs_visit_calls_with_prior_visit,
          $"care_events_partition_date_utc" === $"prior_visit_call_date"
          && upper($"care_events_visit_type") === upper($"prior_visit_visit_type")
          && upper($"call_data_agent_mso") === upper($"prior_visit_agent_mso")
          && upper($"call_data_customer_type") === upper($"prior_visit_customer_type")
          ,"outer")
        .withColumn("call_date",coalesce($"care_events_partition_date_utc",$"call_data_call_date",$"prior_visit_call_date"))
        .withColumn("agent_mso",upper(coalesce($"call_data_agent_mso",$"prior_visit_agent_mso",lit("UNK"))))
        .withColumn("visit_type",upper(coalesce($"care_events_visit_type",$"prior_visit_visit_type",lit("unknown"))))
        .withColumn("customer_type",upper(coalesce($"call_data_customer_type",$"care_events_customer_type",$"prior_visit_customer_type",lit("UNMAPPED"))))
        .withColumn("calls_with_visit",coalesce($"prior_visit_calls_with_visit",lit(0)))
        .withColumn("handled_acct_calls",coalesce($"call_data_handled_acct_calls",lit(0)))
        .withColumn("total_acct_calls",coalesce($"call_data_total_acct_calls",lit(0)))
        .withColumn("total_calls",coalesce($"call_data_total_calls",lit(0)))
        .withColumn("total_acct_visits",coalesce($"care_events_total_visits",lit(0)))
        .withColumn("total_visits",coalesce($"care_events_total_visits",lit(0)))
        .where(
          (lower($"visit_type") === lit("smb") && !$"customer_type".rlike(".*RESI.*")) ||
          isnull($"visit_type") ||
          (lower($"visit_type") === lit("myspectrum") && !$"customer_type".rlike(".*COMM.*")) ||
          (lower($"visit_type") === lit("specnet") && !$"customer_type".rlike(".*COMM.*")) &&
          $"call_date" >= startdate && $"call_date" <= enddate
        )
        .select($"agent_mso",$"visit_type",$"customer_type"
          ,$"calls_with_visit",$"handled_acct_calls",$"total_acct_calls"
          ,$"total_calls",$"total_acct_visits",$"total_visits",$"call_date"
        )
    )

    daysBetween(LocalDate.parse(overwritedate), LocalDate.parse(enddate)).map {
      day =>
        spark.sql(s"ALTER TABLE dev.steve_visit_rate_4calls DROP IF EXISTS PARTITION (call_date = '${day.toString}')")
    }

    {steve_visit_rate_4calls.write
        .format("orc")
        .option("compression", "snappy")
        .partitionBy(
          "call_date"
        )
        .mode(SaveMode.Append)
        .saveAsTable("dev.steve_visit_rate_4calls")}

    val steve_calls_with_visits = (
      steve_calls_with_prior_visit
        .withColumn("call_date",$"call_end_date_utc")
        .groupBy(
          $"account_number"
          ,$"customer_type"
          ,$"customer_subtype"
          ,$"call_inbound_key"
          ,$"product"
          ,$"agent_mso"
          ,$"visit_type"
          ,$"issue_description"
          ,$"issue_category"
          ,$"cause_description"
          ,$"cause_category"
          ,$"resolution_description"
          ,$"resolution_category"
          ,$"resolution_type"
          ,$"call_date"
        )
        .agg(((min($"call_start_div")-max($"visitstart"))/60).alias("minutes_to_call"))
        .select(
          $"account_number"
          ,$"customer_type"
          ,$"customer_subtype"
          ,$"call_inbound_key"
          ,$"product"
          ,$"agent_mso"
          ,$"visit_type"
          ,$"issue_description"
          ,$"issue_category"
          ,$"cause_description"
          ,$"cause_category"
          ,$"resolution_description"
          ,$"resolution_category"
          ,$"resolution_type"
          ,$"minutes_to_call"
          ,$"call_date"
        )
    )

    daysBetween(LocalDate.parse(overwritedate), LocalDate.parse(enddate)).map {
      day =>
        spark.sql(s"ALTER TABLE dev.steve_calls_with_visits DROP IF EXISTS PARTITION (call_date = '${day.toString}')")
    }

    {steve_calls_with_visits.write
        .format("orc")
        .option("compression", "snappy")
        .partitionBy(
          "call_date"
        )
        .mode(SaveMode.Append)
        .saveAsTable("dev.steve_calls_with_visits")}

} catch {
case e: Exception => {
    println(e);
    //Close spark-shell with error
    System.exit(1);
  }
} finally {
  //Close spark-shell
  System.exit(0)
}
