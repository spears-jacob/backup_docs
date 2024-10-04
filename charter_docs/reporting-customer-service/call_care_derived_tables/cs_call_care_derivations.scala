import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import com.spectrum.crypto._

import spark.implicits._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

case class CallsWithVisits(
    account_key: String,
    account_number: String,
    customer_type: String,
    customer_subtype: String,
    call_inbound_key: String,
    product: String,
    agent_mso: String,
    visit_type: String,
    issue_description: String,
    issue_category: String,
    cause_description: String,
    cause_category: String,
    resolution_description: String,
    resolution_category: String,
    resolution_type: String,
    minutes_to_call: Long,
    call_date: String,
    account_agent_mso: String,
    enhanced_mso: Boolean,
    segment_id:String

  )

case class CallsInRate(
    agent_mso: String,
    visit_type: String,
    customer_type: String,
    calls_with_visit: Long,
    handled_acct_calls: Long,
    total_acct_calls: Long,
    total_calls: Long,
    total_acct_visits: Long,
    total_visits: Long,
    call_date: String
    )

try{
  //Setting up the encrypt and decrypt functions
  val encryptString: (String => String) = Aes.encrypt(_)
  val encryptStringUDF = udf(encryptString)
  val encryptString256: (String => String) = Aes.encrypt256(_)
  val encryptString256UDF = udf(encryptString256)

  val decryptString: (String => String) = Aes.decrypt(_)
  val decryptStringUDF = udf(decryptString)
  val decryptString256: (String => String) = Aes.decrypt256(_)
  val decryptString256UDF = udf(decryptString256)

  // val callDataTableName = "cs_call_care_data"
  val callDataTableName = "red_cs_call_care_data_v"
  val callRateTableName = "cs_call_in_rate"
  val callPriorVisitTableName = "cs_calls_with_prior_visits"


  println(s"application id of Spark job is ${spark.conf.get("spark.app.id")}")

  val environment = sys.env("ENVIRONMENT")
  val runDate = sys.env("RUN_DATE")
  val rangeStartDate = sys.env.get("START_DATE")
  val rangeEndDate = sys.env.get("END_DATE")
  /*
  val endDate = "2019-01-30"
  val environment = "dev"
  */

  def createTable[AtomType: Encoder](partitions: String, environment: String, tableName: String):Unit = {
    spark.sqlContext.sql(s"use ${environment}")
    if(!spark.sqlContext.tableNames.contains(tableName)){
      val tableSeq: Seq[AtomType] = Seq()
      tableSeq.toDF.as[AtomType]
          .write
          .format("orc")
          .option("compression", "snappy")
          .partitionBy(
            partitions
          )
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
    }
  }

  def daysBetween(from: LocalDate, to: LocalDate): Seq[LocalDate] = (
      Iterator
          .iterate(from)(_.plusDays(1))
          .take(ChronoUnit.DAYS.between(from, to.plusDays(1)).toInt)
          .toSeq
      )

  val dates: Seq[LocalDate] = if(rangeStartDate.isDefined && rangeEndDate.isDefined){
    daysBetween(LocalDate.parse(rangeStartDate.get), LocalDate.parse(rangeEndDate.get))
  }else {
    Seq(LocalDate.parse(runDate).minusDays(1))
  }

  dates.foreach(date => println(date.toString))

  val backfill = false

      dates.foreach{
        processDate =>
          val endDate = processDate.toString

      val callCareQueryBody =
        // spark.table(s"prod.atom_call_care")
        spark.table(s"prod.red_atom_call_care_v")
          .withColumn("call_end_date_utc", to_date($"call_segment_stop_date_time_in_est"))
      val callCareAtom =
        if(backfill)
        {
          println(s"reading in ${LocalDate.parse(endDate).plusDays(7)}, processing ${endDate}")
          callCareQueryBody.where(to_date($"partition_date") === date_add(lit(endDate), 7) && $"call_end_date_utc" === endDate)
        }else
        {
              callCareQueryBody.where(to_date($"partition_date") === endDate)
        }.select("call_end_date_utc")


      val overwriteDate = {
        callCareAtom.agg(min("call_end_date_utc"))
            .head.getDate(0).toString
      }

      if(callCareAtom.count == 0) {
        println(s"nothing to process for ${endDate}.")
      }else{

//start actual code
        val startTime = System.nanoTime

        //We may need to parse this into case class
        val newCallData = spark
          .table(s"prod.$callDataTableName")
          // .table(s"$environment.$callDataTableName")
          .where($"call_end_date_utc" >= lit(overwriteDate) && $"call_end_date_utc" <= lit(endDate))
          .cache()


       val issueCauseLookup = (
            spark.table(s"prod.cs_issue_cause_lookup")
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

        val issueResolutionLookup = (
            spark.table(s"prod.cs_resolution_lookup")
                .withColumn("l_resolution_description", $"resolution_description")
                .select(
                  $"resolution_category"
                  ,$"resolution_type"
                  ,$"l_resolution_description"
                )
            )

        val msoTransformUDF = udf {
          msoValue: String =>
            msoValue.toUpperCase match {
              case "TWC" => "TWC"
              case "\"TWC\"" => "TWC"
              case "BH" => "BHN"
              case "\"BHN\"" => "BHN"
              case "CHARTER" => "CHR"
              case "CHTR" => "CHR"
              case "\"CHTR\"" => "CHR"
              case "BHN" => "BHN"
              case "NONE" => null
              case "UNKNOWN" => null
              case unknown: String => unknown
              case _ => null
            }
        }

        val quantum_events_portals = (
            spark.table("prod.core_quantum_events_portals_v")
                .select(
                  $"visit__account__account_number"
                  ,$"visit__account__enc_account_number"
                  ,$"visit__visit_id".alias("visit_id")
                  ,$"visit__application_details__application_name"
                  ,$"message__category"
                  ,$"message__name"
                  // TODO: (ghmulli) we need to make sure to use this in newer data. need to figure out when this was added...
                  //$"visit__account__account_billing_id",
                  // TODO: (ghmulli) use message.timestamp ?
                  ,$"received__timestamp"
                  ,$"partition_date_utc"
                  ,$"visit__account__details__mso"
                )
                // .withColumn("visit_account_number",encryptStringUDF(regexp_replace(substring_index(decryptStringUDF($"visit__account__account_number"),"-",-2),"-","")))
                .where(
                  $"partition_date_utc" >= date_sub(lit(overwriteDate), 2).cast("date") &&
                      $"partition_date_utc" <= to_date(lit(endDate)) &&
                      $"visit__account__enc_account_number".isNotNull &&
                      $"visit_id".isNotNull &&
                      $"visit__account__enc_account_number" =!= "dZJho5z9MUSD35BuytdIhg==" && //AMPR: These have been changed to the new encryption scheme
                      $"visit__account__enc_account_number" =!= "Qn+T/sa8AB7Gnxhi4Wx2Xg=="
                )
                .withColumn("received__timestamp", ($"received__timestamp"/1000).cast("BIGINT"))
                .withColumn("visit_type", lower($"visit__application_details__application_name"))
                .withColumn("portals_join_customer_type",
                  when($"visit_type" === "myspectrum" || $"visit_type" === "specnet", "RESIDENTIAL")
                      .otherwise(when($"visit_type" === "smb", "COMMERCIAL")
                          .otherwise("UNMAPPED")))
                .withColumn("visit_mso", when($"visit__account__details__mso".isNotNull, msoTransformUDF($"visit__account__details__mso")))
                .distinct
            ).as("portals").cache



        val cs_calls_and_events = (
          quantum_events_portals
            .join(
              newCallData.select(
                "encrypted_padded_account_number_256"
                                //,"account_number"
                ,"encrypted_account_number_256"
                ,"account_agent_mso"
                ,"call_inbound_key"
                ,"enhanced_account_number")
                .distinct
                .as("left_call_data")
                .where($"enhanced_account_number" === lit(false)).as("portal_atom_join")
               ,$"visit__account__enc_account_number" ===$"encrypted_account_number_256"
              // ,$"visit_account_number" === encryptStringUDF(decryptString256UDF($"account_number"))
                && $"visit_mso" === $"account_agent_mso"
              ,"inner"
            )
            .join(
              newCallData.as("right_call_data"),
              Seq("call_inbound_key", "account_agent_mso"),
              "inner"
            )
            .where(
              (($"call_start_timestamp_utc"/1000) - $"received__timestamp") <= "86400"
                && (($"call_start_timestamp_utc"/1000) - $"received__timestamp") >= 0)
            .distinct
          .select(
              // $"visit_account_number"
              $"visit__account__enc_account_number"
              ,$"visit_id"
              ,$"visit_mso"
              ,$"message__category" // do we event use that visit_customer_type?
              ,$"message__name"
              ,$"received__timestamp"
              ,$"partition_date_utc"
              ,$"visit_type"
              ,$"right_call_data.agent_effective_hire_date"
              ,$"right_call_data.agent_job_role_name"
              // ,$"right_call_data.account_number"
              ,$"right_call_data.encrypted_account_number_256"
              ,$"right_call_data.encrypted_padded_account_number_256"
              ,$"right_call_data.call_end_date_utc"
              ,$"right_call_data.call_end_datetime_utc"
              ,$"right_call_data.call_end_time_utc"
              ,$"right_call_data.call_end_timestamp_utc"
              ,$"call_id"
              ,$"call_inbound_key"
              ,$"right_call_data.call_start_date_utc"
              ,$"right_call_data.call_start_datetime_utc"
              ,$"right_call_data.call_start_time_utc"
              ,$"right_call_data.call_start_timestamp_utc"
              ,$"right_call_data.call_type"
              ,$"right_call_data.care_center_management_name"
              ,$"right_call_data.cause_description"
              ,$"right_call_data.company_code"
              ,$"right_call_data.encrypted_created_by_256"
              ,$"right_call_data.created_on"
              // ,$"right_call_data.customer_account_number"
              ,$"encrypted_customer_account_number_256"
              ,$"right_call_data.segment_handled_flag"
              ,$"right_call_data.customer_subtype"
              ,$"portals_join_customer_type".alias("customer_type")
              ,$"right_call_data.eduid"
              ,$"right_call_data.issue_description"
              ,$"right_call_data.last_handled_segment_flag"
              ,$"right_call_data.location_name"
              // ,$"right_call_data.notes_txt"
              ,$"encrypted_notes_txt_256"
              // ,$"right_call_data.phone_number_from_tracker"
              ,$"encrypted_phone_number_from_tracker_256"
              ,$"right_call_data.previous_call_time_utc"
              ,$"right_call_data.product"
              ,$"right_call_data.record_update_timestamp"
              ,$"right_call_data.resolution_description"
              ,$"right_call_data.segment_duration_minutes"
              ,$"right_call_data.segment_duration_seconds"
              ,$"right_call_data.segment_end_datetime_utc"
              ,$"right_call_data.segment_end_time_utc"
              ,$"right_call_data.segment_end_timestamp_utc"
              ,$"right_call_data.segment_number"
              ,$"right_call_data.segment_start_datetime_utc"
              ,$"right_call_data.segment_start_time_utc"
              ,$"right_call_data.segment_start_timestamp_utc"
              ,$"right_call_data.service_call_tracker_id"
              ,$"right_call_data.split_sum_desc"
              ,$"right_call_data.truck_roll_flag"
              // ,$"right_call_data.account_key"
              ,$"encrypted_account_key_256"
              ,$"portal_atom_join.account_agent_mso"
              ,$"right_call_data.enhanced_mso"
              ,$"right_call_data.segment_id")
          ).distinct
          .as("call_and_event")

        //TODO adding account_key may cause a split
        val steve_calls_with_prior_visit = (
            cs_calls_and_events
                .where($"segment_handled_flag" === lit(true))
                .join(issueCauseLookup,
                  upper($"issue_description") === $"icl_issue_description"
                      && upper($"cause_description") === $"icl_cause_description"
                      && upper($"call_type") === $"icl_call_group",
                  "left")
                .join(issueResolutionLookup,
                  upper($"resolution_description") === $"l_resolution_description",
                  "left")
                .groupBy(
                  // $"account_key"
                  $"encrypted_account_key_256"
                  // ,$"account_number"
                  ,$"encrypted_padded_account_number_256"
                  ,$"visit_type"
                  ,$"call_inbound_key"
                  ,$"customer_type"
                  ,$"customer_subtype"
                  ,$"visit_mso"
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
                  ,$"account_agent_mso"
                  ,$"enhanced_mso"
                  ,$"segment_id"
                )
                .agg(min($"received__timestamp").alias("visitStart"), max($"received__timestamp").alias("visitEnd"))
                .where($"visitStart" < ($"call_start_timestamp_utc" / 1000)
                    && ($"visitStart" > ($"previous_call_time_utc" / 1000) || $"previous_call_time_utc".isNull))
                .withColumn("call_start_div", ($"call_start_timestamp_utc"/1000) )
                .select(
                  // $"account_key"
                  $"encrypted_account_key_256"
                  // ,$"account_number"
                  ,$"encrypted_padded_account_number_256"
                  ,$"call_inbound_key"
                  ,$"customer_type"
                  ,$"customer_subtype"
                  ,$"visit_mso"
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
                  ,$"account_agent_mso"
                  ,$"enhanced_mso"
                  ,$"segment_id"
                )
            ).as("prior_visit")

        //Load table that were formerly call_in_rate views
        val cs_visit_rate_steve_care_events = (
            quantum_events_portals
                .where($"visit__account__enc_account_number".isNotNull
                    && $"visit_id".isNotNull)
                .withColumn("care_events_partition_date_utc",$"partition_date_utc")
                .withColumn("care_events_customer_type",upper($"portals_join_customer_type"))
                .withColumn("care_events_visit_type",upper($"visit_type"))
                .withColumn("care_events_visit_mso", $"visit_mso")
                .groupBy($"care_events_partition_date_utc",$"care_events_customer_type",$"care_events_visit_type", $"care_events_visit_mso")
                .agg(countDistinct($"visit__account__enc_account_number", $"care_events_visit_mso").alias("care_events_total_acct_visits")
                  ,countDistinct($"visit_id").alias("care_events_total_visits")
                )
            ).as("cs_visit_rate_care")

        val cs_visit_rate_call_data = (
          newCallData
            .withColumn("call_data_call_date",$"call_end_date_utc")
            .withColumn("call_data_agent_mso", $"account_agent_mso")
            .withColumn("call_data_customer_type",upper($"customer_type"))
            .where($"segment_handled_flag" === lit(true)
              && $"enhanced_account_number" === lit(false))
            .groupBy($"call_data_call_date",$"call_data_agent_mso",$"call_data_customer_type")
            .agg(countDistinct(when($"enhanced_account_number" === lit(false),$"encrypted_padded_account_number_256")).alias("call_data_total_acct_calls")
              ,countDistinct($"call_inbound_key").alias("call_data_total_calls")
              ,countDistinct(when($"encrypted_padded_account_number_256" =!= "+yzQ1eS5iRmWnflCmvNlSg==" && $"enhanced_account_number" === lit(false), $"call_inbound_key")).alias("call_data_handled_acct_calls")
            )
          ).as("cs_visit_rate")

        val cs_visit_calls_with_prior_visit = (
          steve_calls_with_prior_visit
            .withColumn("prior_visit_call_date",$"call_end_date_utc")
            .withColumn("prior_visit_visit_type",upper($"visit_type"))
            .withColumn("prior_visit_mso", $"visit_mso")
            .withColumn("prior_visit_customer_type",upper($"customer_type"))
            .groupBy($"prior_visit_call_date",$"prior_visit_mso",$"prior_visit_customer_type",$"prior_visit_visit_type")
            .agg(countDistinct($"call_inbound_key").alias("prior_visit_calls_with_visit")
            )
          ).as("cs_visit_prior_visit")


        val steve_visit_rate_4calls = (
            cs_visit_rate_steve_care_events
                .join(cs_visit_rate_call_data,
                  $"care_events_partition_date_utc" === $"call_data_call_date"
                      && upper($"care_events_customer_type") === upper($"call_data_customer_type")
                      && $"care_events_visit_mso" === $"call_data_agent_mso"
                  ,"outer")
                .join(cs_visit_calls_with_prior_visit,
                  $"care_events_partition_date_utc" === $"prior_visit_call_date"
                      && upper($"care_events_visit_type") === upper($"prior_visit_visit_type")
                      && $"care_events_visit_mso" === $"prior_visit_mso"
                      && upper($"call_data_customer_type") === upper($"prior_visit_customer_type")
                  ,"outer")
                .withColumn("call_date",coalesce($"care_events_partition_date_utc", $"call_data_call_date", $"prior_visit_call_date"))
                .withColumn("agent_mso",upper(coalesce($"care_events_visit_mso", $"prior_visit_mso", $"call_data_agent_mso",lit("UNMAPPED"))))
                .withColumn("visit_type",upper(coalesce($"care_events_visit_type",$"prior_visit_visit_type",lit("UNKNOWN"))))
                .withColumn("customer_type",upper(coalesce($"care_events_customer_type", $"prior_visit_customer_type", $"call_data_customer_type", lit("UNMAPPED"))))
                .withColumn("calls_with_visit",coalesce($"prior_visit_calls_with_visit",lit(0)))
                .withColumn("handled_acct_calls",coalesce($"call_data_handled_acct_calls",lit(0)))
                .withColumn("total_acct_calls",coalesce($"call_data_total_acct_calls",lit(0)))
                .withColumn("total_calls",coalesce($"call_data_total_calls",lit(0)))
                .withColumn("total_acct_visits",coalesce($"care_events_total_acct_visits",lit(0)))
                .withColumn("total_visits",coalesce($"care_events_total_visits",lit(0)))
                .select($"agent_mso",$"visit_type",$"customer_type"
                  ,$"calls_with_visit",$"handled_acct_calls",$"total_acct_calls"
                  ,$"total_calls",$"total_acct_visits",$"total_visits",$"call_date"
                ).where($"call_date" >= overwriteDate
                && $"call_date" <= endDate)
            ).as("rate4calls")

        createTable[CallsInRate]("call_date", environment, callRateTableName)

        daysBetween(LocalDate.parse(overwriteDate), LocalDate.parse(endDate)).foreach(day =>
          spark.sql(s"ALTER TABLE ${environment}.${callRateTableName} DROP IF EXISTS PARTITION (call_date = '${day}')")
        )

        println(s"writing ${environment}.${callRateTableName} for data between ${overwriteDate} and ${endDate}")


        {steve_visit_rate_4calls.write
            .format("orc")
            .option("compression", "snappy")
            .partitionBy(
              "call_date"
            )
            .mode(SaveMode.Append)
            .saveAsTable(s"${environment}.${callRateTableName}")}


        //TODO We are bringing in account_key, this may split the group by,
        val steve_calls_with_visits = (
            steve_calls_with_prior_visit
                .withColumn("call_date",$"call_end_date_utc")
                .withColumn("account_number",$"encrypted_padded_account_number_256")
                .withColumn("account_key",$"encrypted_account_key_256")
                .groupBy(
                  // $"encrypted_account_key_256"
                  $"account_key"
                  // ,$"encrypted_padded_account_number_256"
                  ,$"account_number"
                  ,$"customer_type"
                  ,$"customer_subtype"
                  ,$"call_inbound_key"
                  ,$"product"
                  ,$"visit_mso"
                  ,$"visit_type"
                  ,$"issue_description"
                  ,$"issue_category"
                  ,$"cause_description"
                  ,$"cause_category"
                  ,$"resolution_description"
                  ,$"resolution_category"
                  ,$"resolution_type"
                  ,$"call_date"
                  ,$"account_agent_mso"
                  ,$"enhanced_mso"
                  ,$"segment_id"
                )
                .agg(((min($"call_start_div")-max($"visitstart"))/60).alias("minutes_to_call"))
                .select(
                  $"account_key"
                  // $"encrypted_account_key_256"
                  ,$"account_number"
                  // ,$"encrypted_padded_account_number_256"
                  ,$"customer_type"
                  ,$"customer_subtype"
                  ,$"call_inbound_key"
                  ,$"product"
                  ,$"visit_mso".alias("agent_mso")
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
                  ,$"account_agent_mso"
                  ,$"enhanced_mso"
                  ,$"segment_id"
                )
            )

        createTable[CallsWithVisits]("call_date", environment, callPriorVisitTableName)

        daysBetween(LocalDate.parse(overwriteDate), LocalDate.parse(endDate)).foreach(day =>
          spark.sql(s"ALTER TABLE ${environment}.${callPriorVisitTableName} DROP IF EXISTS PARTITION (call_date = '${day}')")
        )

        println(s"writing ${environment}.${callPriorVisitTableName} for data between ${overwriteDate} and ${endDate}")

        {steve_calls_with_visits.write
            .format("orc")
            .option("compression", "snappy")
            .partitionBy(
              "call_date"
            )
            .mode(SaveMode.Append)
            .saveAsTable(s"${environment}.${callPriorVisitTableName}")}

        println(s"  - [ elapsed time = ${(System.nanoTime - startTime) / 1e9} seconds for ${endDate}]")
      }// end of processing
  }

} catch {
  case e: Exception =>
    println(s"exception occurred - \n ${e.getCause}")
    e.printStackTrace()
    System.exit(1)
}
//Close spark-shell
System.exit(0)
