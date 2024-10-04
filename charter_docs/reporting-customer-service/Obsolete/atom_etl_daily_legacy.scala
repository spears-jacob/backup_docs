import org.apache.spark.sql.expressions._
import com.spectrum.crypto._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}
import org.apache.spark
import org.apache.spark.sql._
import java.sql._

import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.sun.jdi.LongType

def convertTo[T <: Product : Encoder]: DataFrame => Dataset[T] =
  ( (df: DataFrame) => {
    df.select(
      implicitly[Encoder[T]].schema.fieldNames
        .map(col(_)): _* )
      .as[T]
  } ) // enforce projection down to only the columns in the case class (since .as[T] doesn't)



//Setting case classes to be used in the process
case class CallCareAtom(account_key: String, account_number_encrypted: Option[String], answered_disposition_description: String,
                        answered_disposition_id: String, call_cause_description: Option[String], call_handled_flag: Boolean, call_inbound_key: String,
                        call_issue_description: Option[String], call_notes_text: Option[String], call_owner_name: Option[String],
                        call_resolution_description: Option[String], call_id: String, call_segment_number: String,
                        call_type_code: Option[String], care_center_management_name: String, legacy_corp_code: String, created_by: Option[String],
                        created_on_date: String, customer_account_number_encrypted: String, customer_call_center_flag: Option[Boolean],
                        edu_id: Option[String], effective_hire_date: Option[String], job_role_name: Option[String], location_name: String,
                        last_handled_segmentation_flag: Boolean, last_updated_date_time_stamp: String, mso_agent_name: Option[String],
                        product_lob_description: Option[String], call_segment_stop_date_time: String, call_segment_stop_date_time_in_utc: String,
                        call_segment_start_date_time: String, call_segment_start_date_time_in_utc: String, split_sum_description: Option[String],
                        service_call_tracker_id: Option[String], truck_roll_flag: Boolean, track_phone_number_encrypted: Option[String],
                        unified_call_id: String, partition_date: String, call_end_date_utc: String)

case class AccountData(account_key: String,
                       encrypted_legacy_account_number_256: String,
                       partition_date_denver: String,
                       customer_type: String,
                       legacy_company: String)

case class CallData(
                     call_inbound_key: String
                     ,call_id: String
                     ,call_start_date_utc: String
                     ,call_start_time_utc: String
                     ,call_end_time_utc: String
                     ,call_start_datetime_utc: String
                     ,call_end_datetime_utc: String
                     ,call_start_timestamp_utc: Long
                     ,call_end_timestamp_utc: Long
                     ,previous_call_time_utc: Option[Long]
                     ,segment_id: String
                     ,segment_number: String
                     ,segment_status_disposition: String
                     ,segment_start_time_utc: String
                     ,segment_end_time_utc: String
                     ,segment_start_datetime_utc: String
                     ,segment_end_datetime_utc: String
                     ,segment_start_timestamp_utc: Long
                     ,segment_end_timestamp_utc: Long
                     ,segment_duration_seconds: Long
                     ,segment_duration_minutes: Double
                     ,segment_handled_flag: Boolean
                     ,customer_call_count_indicator: String
                     ,call_handled_flag: Boolean
                     ,call_owner: String
                     ,product: String
                     ,account_number: String
                     ,account_key: String
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
                     ,account_agent_mso: String
                     ,enhanced_mso: Boolean
                   )

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

  val callDataTableName = "cs_call_care_data"
  val callRateTableName = "cs_call_in_rate"
  val callPriorVisitTableName = "cs_calls_with_prior_visits"

  val accountAtom  = "prod.atom_accounts_snapshot"


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
        spark.table(s"prod.atom_call_care")
          .withColumn("call_id", trim($"caller_id"))
          .withColumn("call_segment_number", $"call_segmentation_number")
          .withColumn("call_segment_start_date_time_in_utc",
            to_utc_timestamp(substring($"call_segment_start_date_time_in_est", 0, 19), "America/New_York"))
          .withColumn("call_segment_stop_date_time_in_utc",
            to_utc_timestamp(substring($"call_segment_stop_date_time_in_est", 0, 19), "America/New_York"))
          .withColumn("call_end_date_utc", to_date($"call_segment_stop_date_time_in_est"))
          .withColumn("account_key", decryptStringUDF($"account_id_encrypted"))


      val callCareAtom =
        if(backfill)
        {
          println(s"reading in ${LocalDate.parse(endDate).plusDays(7)}, processing ${endDate}")
          callCareQueryBody.where(to_date($"partition_date") === date_add(lit(endDate), 7) && $"call_end_date_utc" === endDate)
        }else
          {
            callCareQueryBody.where(to_date($"partition_date") === endDate)
          }.transform(convertTo[CallCareAtom])


      val overwriteDate = {
        callCareAtom.agg(min("call_end_date_utc"))
          .head.getDate(0).toString
      }

      if(callCareAtom.count == 0) {
        println(s"nothing to process for ${endDate}.")
      }else{

        val startTime = System.nanoTime

        val accountsForDate = {spark.table("prod.quantum_atom_snapshot_accounts_v")
          .withColumn("customer_type_extended", when($"extract_type" === "COMMERCIAL_BUSINESS", "COMMERCIAL").otherwise("RESIDENTIAL"))
          .select(
            $"account_key",
            $"partition_date_denver",
            $"customer_type_extended".alias("customer_type"),
            $"encrypted_legacy_account_number_256",
            $"legacy_company")
          //,concat_ws("::", $"extract_source", $"extract_type", $"legacy_company", $"customer_type").as("customer_type")  DON'T KNOW IF THIS IS NEEDED
          .where($"partition_date_denver" >= lit(overwriteDate)
          && $"partition_date_denver" <= lit(endDate))
          .distinct
          .as[AccountData]}

        val lastHandledFlag = {callCareAtom
          .where($"call_handled_flag" === true
            && $"customer_call_center_flag" === true
            && $"call_owner_name" === "Customer Operations"
            && $"answered_disposition_description" === "Answered by an agent")
          .select(
            $"call_inbound_key".alias("call_inbound_key_joining_column"),
            $"call_segment_start_date_time_in_utc".alias("call_segment_start_date_time_in_utc_joining_column")
          )
          .groupBy("call_inbound_key_joining_column")
          .agg(
            max("call_segment_start_date_time_in_utc_joining_column").alias("call_segment_start_date_time_in_utc_joining_column")
          )
          .withColumn("last_handled_flag_correction", lit(true))
          .distinct
          .as("lastHandledFlag")
        }

        //Get first CallStart time and last CallEnd time by call inbound key for call level attributes
        val callStartStopTimes = (
          callCareAtom
            .select(
              $"call_inbound_key",
              $"call_segment_start_date_time_in_utc",
              $"call_segment_stop_date_time_in_utc",
              $"account_number_encrypted",
              $"account_key"
            )
            .groupBy("call_inbound_key")
            .agg(
              min($"call_segment_start_date_time_in_utc").alias("call_start_date_time_utc"),
              max($"call_segment_stop_date_time_in_utc").alias("call_end_date_time_utc"),
              first($"account_number_encrypted", true).alias("account_number_encrypted_correction"), // STN returns the first account number that is not null, test in test_script
              first($"account_key", true).alias("account_key_correction")
            )
            .select(
              $"call_inbound_key",
              $"call_start_date_time_utc".alias("call_start_date_time_in_utc_new"),
              $"call_end_date_time_utc".alias("call_end_date_time_in_utc_new"),
              $"account_number_encrypted_correction", // removed 256 since it is non 256 encryption
              $"account_key_correction"
            )
          ).as("startStopTimes")


        val callAtomWithCallStartStop = (
          callCareAtom.join(
            callStartStopTimes,
            Seq("call_inbound_key"),
            "left"
          )
            .join(
              lastHandledFlag,
              lastHandledFlag("call_inbound_key_joining_column") === callCareAtom("call_inbound_key")
                && lastHandledFlag("call_segment_start_date_time_in_utc_joining_column") === callCareAtom("call_segment_start_date_time_in_utc"),
              "left"
            )
          ).as("atomWithStartStopTimes")

        val segment_start_window = Window.partitionBy($"call_inbound_key").orderBy($"call_segment_start_date_time_in_utc")

        //Segment End Date Time is calculated by taking the next segments Start Date time and subtracting 1 second from the value
        val segment_end_times = (
          callAtomWithCallStartStop
            .withColumn(
              "segment_end_datetime_correction",
              lead($"call_segment_start_date_time_in_utc", 1).over(segment_start_window)
            )
            .select(
              $"call_inbound_key".alias("segment_call_inbound_key_joining_column"),
              $"call_segment_start_date_time_in_utc".alias("call_segment_start_date_time_in_utc_joining_column"),
              $"customer_call_center_flag".alias("customer_call_center_flag_joining_column"),
              $"answered_disposition_description".alias("answered_disposition_description_joining_column"),
              $"call_owner_name".alias("call_owner_name_joining_column"),
              $"call_handled_flag".alias("call_handled_flag_joining_column"),
              from_unixtime(
                unix_timestamp($"segment_end_datetime_correction").minus(1), "YYYY-MM-dd HH:mm:ss"
              ).alias("segment_end_datetime_correction")
            )
            .distinct
          ).as("endTimes")

        // STN Originally not using enough columns to uniquely identify a segment which was adding more rows
        val callAtomWithEndTimesColumnAndCallStartStop = (
          callAtomWithCallStartStop
            .join(
              segment_end_times,
              segment_end_times("segment_call_inbound_key_joining_column") === callAtomWithCallStartStop("call_inbound_key")
                && segment_end_times("call_segment_start_date_time_in_utc_joining_column") === callAtomWithCallStartStop("call_segment_start_date_time_in_utc")
                && segment_end_times("customer_call_center_flag_joining_column") === callAtomWithCallStartStop("customer_call_center_flag")
                && segment_end_times("answered_disposition_description_joining_column") === callAtomWithCallStartStop("answered_disposition_description")
                && segment_end_times("call_handled_flag_joining_column") === callAtomWithCallStartStop("call_handled_flag")
                && segment_end_times("call_owner_name_joining_column") === callAtomWithCallStartStop("call_owner_name"),
              "left"
            )
          ).as("atomWithEndTimes")

        val last_handled_segment_window = Window.partitionBy($"call_inbound_key").orderBy(when($"segment_handled_flag" === 1, 1).otherwise(0) desc,$"segment_start_date_time_utc" desc)

        //Selecting out of managed and into TMP while joining to previously created datasets for cleansing
        val cca_joined = {
          callAtomWithEndTimesColumnAndCallStartStop
            // trimmed above .withColumn("call_id", regexp_replace($"call_id","\\s+",""))
            .withColumn("call_start_date_time_utc",substring($"call_start_date_time_in_utc_new",0,19))
            .withColumn("call_start_date_utc",to_date($"call_start_date_time_utc"))
            .withColumn("call_end_date_time_utc",substring($"call_end_date_time_in_utc_new",0,19))
            .withColumn("call_end_date_utc",to_date($"call_end_date_time_utc"))
            .withColumn("call_start_time_utc", substring($"call_start_date_time_utc",12,8))
            .withColumn("call_end_time_utc", substring($"call_end_date_time_utc",12,8))
            .withColumn("call_start_timestamp_utc",unix_timestamp(concat($"call_start_date_time_utc",lit(" UTC")),"yyyy-MM-dd HH:mm:ss zzz") * 1000)
            .withColumn("call_end_timestamp_utc",unix_timestamp(concat($"call_end_date_time_utc",lit(" UTC")),"yyyy-MM-dd HH:mm:ss zzz") * 1000)
            .withColumn("call_end_date_denver", to_date(from_utc_timestamp($"call_end_date_time_utc", "America/Denver")))
            .withColumn("previous_call_time_utc",lit(0))
            .withColumn("segment_id",concat($"call_inbound_key", lit("-"), $"call_id", lit("-"), $"call_segment_number"))
            .withColumn("segment_start_date_time_utc",substring($"call_segment_start_date_time_in_utc",0,19))
            .withColumn("segment_end_date_time_utc",substring($"segment_end_datetime_correction",0,19))
            .withColumn("segment_start_time_utc",substring($"segment_start_date_time_utc",12,8))
            .withColumn("segment_end_time_utc",substring($"segment_end_date_time_utc",12,8))
            .withColumn("segment_start_timestamp_utc",unix_timestamp(concat($"segment_start_date_time_utc",lit(" UTC")),"yyyy-MM-dd HH:mm:ss zzz") * 1000)
            .withColumn("segment_end_timestamp_utc",unix_timestamp(concat($"segment_end_date_time_utc",lit(" UTC")),"yyyy-MM-dd HH:mm:ss zzz") * 1000)
            .withColumn("segment_duration_seconds",unix_timestamp($"segment_end_date_time_utc")-unix_timestamp($"segment_start_date_time_utc"))
            .withColumn("segment_duration_minutes",$"segment_duration_seconds"/60)
            .withColumn("call_handled_flag",$"call_handled_flag".cast("int"))
            .withColumn("call_owner",coalesce($"call_owner_name",lit("Unknown")))
            .withColumn("segment_status_disposition",$"answered_disposition_description")
            .withColumn("segment_handled_flag", when($"call_handled_flag" === true && lower($"call_owner") === "customer operations" && lower($"segment_status_disposition") === "answered by an agent",1).otherwise(0))
            .withColumn("customer_call_count_indicator",$"customer_call_center_flag")
            .withColumn("product",coalesce($"product_lob_description",lit("Unknown")))
            .withColumn("account_number", $"account_number_encrypted_correction")
            .withColumn("account_key", $"account_key_correction")
            .withColumn("company_code",$"legacy_corp_code")
            .withColumn("created_on_date", date_format(to_timestamp($"created_on_date", "yyyy-MM-dd HH:mm:ss"), "MM/dd/yyyy HH:mm:ss"))
            .withColumn("agent_job_role_name",$"job_role_name")
            .withColumn("agent_effective_hire_date",when($"effective_hire_date" === "1900/01/01",null).otherwise(regexp_replace($"effective_hire_date","/","-")))
            .withColumn("phone_number_from_tracker",decryptStringUDF($"track_phone_number_encrypted"))
            .withColumn("agent_mso", upper($"mso_agent_name"))
            .withColumn("last_handled_segment_flag", when($"last_handled_flag_correction", true).otherwise(false))
            .withColumn("last_updated_date_time_stamp", date_format(to_timestamp($"last_updated_date_time_stamp", "yyyy-MM-dd HH:mm:ss"), "MM/dd/yyyy HH:mm:ss.SSS" ))
            .withColumn("source", lit("atom"))
            .withColumn("enhanced_account_number", lit(false))
        }

        //this is only differance in this file compare to atom_etl_daily. Legacy data does not have account_key, so we are joining based on account_number
        val callsToEnhance = {
          cca_joined
            .join(
              accountsForDate,
              decryptStringUDF($"account_number") === decryptString256UDF($"encrypted_legacy_account_number_256")
                //accountsForDate("account_key") === $"account_key_correction"
                && $"call_end_date_denver" === $"partition_date_denver",
              "left"
            )
            .withColumn("customer_type_with_unmapped", when($"customer_type".isNotNull, upper($"customer_type"))
              .otherwise(lit("UNMAPPED")))
            .withColumn("customer_subtype", lit(null))
            .withColumn("account_agent_mso", upper(coalesce($"legacy_company", $"agent_mso")))
            .withColumn("enhanced_mso",
              when($"legacy_company".isNotNull, true)
                .otherwise(false))
            .select(
              $"account_key_correction".alias("account_key")
              ,$"call_inbound_key"
              ,$"call_id"
              ,$"call_start_date_utc"
              ,$"call_start_time_utc"
              ,$"call_end_time_utc"
              ,$"call_start_date_time_utc".alias("call_start_datetime_utc")
              ,$"call_end_date_time_utc".alias("call_end_datetime_utc")
              //,$"call_start_date_time_utc"
              //,$"call_end_date_time_utc"
              ,$"call_start_timestamp_utc"
              ,$"call_end_timestamp_utc"
              ,$"previous_call_time_utc"
              ,$"segment_id"
              ,$"call_segment_number".alias("segment_number")
              //,$"call_segment_number"
              ,$"segment_status_disposition"
              ,$"segment_start_time_utc"
              ,$"segment_end_time_utc"
              ,$"segment_start_date_time_utc".alias("segment_start_datetime_utc")
              ,$"segment_end_date_time_utc".alias("segment_end_datetime_utc")
              //,$"segment_start_date_time_utc"
              //,$"segment_end_date_time_utc"
              ,$"segment_start_timestamp_utc"
              ,$"segment_end_timestamp_utc"
              ,$"segment_duration_seconds"
              ,$"segment_duration_minutes".cast(DoubleType)
              ,$"segment_handled_flag"
              ,$"customer_call_count_indicator".cast(BooleanType)
              ,$"call_handled_flag"
              ,$"call_owner"
              ,$"product"
              ,$"encrypted_legacy_account_number_256".alias("account_number")
              ,$"customer_account_number_encrypted".alias("customer_account_number")
              //,$"customer_account_number_encrypted"
              ,$"customer_type_with_unmapped".alias("customer_type")
              ,$"customer_subtype".cast(StringType)
              ,$"truck_roll_flag"
              ,$"call_notes_text".alias("notes_txt")
              //,$"call_notes_text"
              ,$"call_resolution_description".alias("resolution_description")
              ,$"call_cause_description".alias("cause_description")
              ,$"call_issue_description".alias("issue_description")
              //,$"call_resolution_description"
              //,$"call_cause_description"
              //,$"call_issue_description"
              ,$"company_code"
              ,$"service_call_tracker_id"
              ,$"created_on_date".alias("created_on")
              //,$"created_on_date"
              ,$"created_by"
              ,$"phone_number_from_tracker"
              ,$"call_type_code".alias("call_type")
              ,$"split_sum_description".alias("split_sum_desc")
              //,$"call_type_code"
              //,$"split_sum_description"
              ,$"location_name"
              ,$"care_center_management_name"
              ,$"agent_job_role_name"
              ,$"agent_effective_hire_date"
              ,$"agent_mso"
              ,$"edu_id".alias("eduid")
              //,$"edu_id"
              ,$"last_handled_segment_flag"
              ,$"last_updated_date_time_stamp".alias("record_update_timestamp")
              //,$"last_updated_date_time_stamp"
              ,$"source"
              ,$"enhanced_account_number"
              ,$"call_end_date_utc"
              ,$"account_agent_mso"
              ,$"enhanced_mso"
            ).as[CallData]
        }

        /*
              val singlePhoneAccounts = {
                spark.table(s"${environment}.steve_call_data")
                    .withColumn("unencrypted_account_number", decryptString256UDF($"account_number"))
                    .where(length($"phone_number_from_tracker") === 10
                        && !(substring($"phone_number_from_tracker",1,1).isin(0,1))
                        && $"enhanced_account_number" === false
                        && lower($"unencrypted_account_number") =!= "unknown"
                        && !isnull($"unencrypted_account_number"))
                    // filter out phone numbers that are not 10 digits or start with 0 and 1 or are unknown
                    // filter out enhanced account numbers, we do not want to enhance with a previously enhanced value
                    .select($"call_inbound_key", $"phone_number_from_tracker", $"account_number", $"call_end_date_utc").distinct
                    // count the distinct account numbers, get the last call_end_date for that phone number, and use max(account_number) to get the account number
                    .groupBy($"phone_number_from_tracker")
                    .agg(countDistinct($"account_number").alias("account_count"), max($"call_end_date_utc").alias("most_recent_call_date"), max($"account_number").alias("acct_num"))
                    // filter to only results where the count of distinct account numbers is 1 and select out phone_number
                    .where($"account_count" === 1 && $"most_recent_call_date" >= add_months(lit(overwriteDate), -6))
                    .select($"phone_number_from_tracker".alias("phone_number"), $"acct_num")
              }

              val enhanced_call_data = {
                callsToEnhance
                    .withColumn("unencrypted_account_number", coalesce(decryptString256UDF($"account_number"), lit("unknown")))
                    // join enhanceable accounts back to call data using a left join
                    .join(
                  singlePhoneAccounts, $"phone_number" === $"phone_number_from_tracker" && lower($"unencrypted_account_number") === "unknown", "left"
                )
                    // if there is a value in enhanceable accounts, update both account_number and enhanced_account_number flag
                    .withColumn("account_number", coalesce($"account_number", $"acct_num"))
                    .withColumn("enhanced_account_number", when($"acct_num".isNotNull && $"account_number".isNull , lit(true)).otherwise(lit(false)))
                    .as[CallData]
              }
        */


        //TODO Should we change the partitionBy column here??
        val previous_call_time_window = Window.partitionBy($"account_key").orderBy($"call_end_timestamp_utc", $"call_inbound_key")

        // should use account key for this join, it would be more accurate
        val previous_call_time = {
          callsToEnhance.select($"call_inbound_key", $"account_key", $"call_end_timestamp_utc")
            // STN can filter on account_key =!= "-1" (same count as account_number =!= "unknown"
            .where(lower($"account_key") =!= "-1"
            && $"account_key".isNotNull)
            .select($"call_inbound_key", $"account_key", $"call_end_timestamp_utc").distinct
            .withColumn("previous_call_time_utc", lag($"call_end_timestamp_utc", 1).over(previous_call_time_window))
            .select($"account_key".alias("pre_call_acct_key"),
              $"call_inbound_key".alias("previous_time_call_inbound_key"),
              $"previous_call_time_utc")
        }

        val newCallData = {callsToEnhance
          .drop($"previous_call_time_utc")
          // join call data to previous_call_time to get previous call time
          .join(
          previous_call_time,
          $"call_inbound_key" === $"previous_time_call_inbound_key"
            && $"pre_call_acct_key" === $"account_key",
          "left"
        ).where($"call_end_date_utc" >= lit(overwriteDate) && $"call_end_date_utc" <= lit(endDate))
          .drop("phone_number", "acct_num", "pre_call_acct_key", "previous_time_call_inbound_key")
          .as[CallData]}.cache

        createTable[CallData]("call_end_date_utc", environment, callDataTableName)

        daysBetween(LocalDate.parse(overwriteDate), LocalDate.parse(endDate)).foreach(day =>
          spark.sql(s"ALTER TABLE ${environment}.${callDataTableName} DROP IF EXISTS PARTITION (call_end_date_utc = '${day}')")
        )

        println(s"writing ${environment}.${callDataTableName} for data between ${overwriteDate} and ${endDate}")

        {newCallData.as[CallData].write.format("orc").option("compression", "snappy")
          .partitionBy("call_end_date_utc")
          .mode(SaveMode.Append).saveAsTable(s"${environment}.${callDataTableName}")
        }

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
              ,$"visit__account__details__mso"
            )
            .withColumn("visit_account_number",encryptStringUDF(regexp_replace(substring_index(decryptStringUDF($"visit__account__account_number"),"-",-2),"-","")))
            .where(
              $"partition_date_utc" >= date_sub(lit(overwriteDate), 2).cast("date") &&
                $"partition_date_utc" <= to_date(lit(endDate)) &&
                $"visit_account_number".isNotNull &&
                $"visit_id".isNotNull &&
                $"visit_account_number" =!= "GSWNkZXIfDPD6x25Na3i8g==" &&
                $"visit_account_number" =!= "7FbKtybuOWU4/Q0SRInbHA=="
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
          venona_events_portals
            .join(
              newCallData.select("account_number"
                ,"account_agent_mso"
                ,"call_inbound_key"
                ,"enhanced_account_number")
                .distinct
                .as("left_call_data")
                .where($"enhanced_account_number" === lit(false)).as("portal_atom_join")
              ,$"visit_account_number" === encryptStringUDF(decryptString256UDF($"account_number"))
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
              $"visit_account_number"
              ,$"visit_id"
              ,$"visit_mso"
              ,$"message__category" // do we event use that visit_customer_type?
              ,$"message__name"
              ,$"received__timestamp"
              ,$"partition_date_utc"
              ,$"visit_type"
              ,$"right_call_data.agent_effective_hire_date"
              ,$"right_call_data.agent_job_role_name"
              ,$"right_call_data.account_number"
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
              ,$"right_call_data.created_by"
              ,$"right_call_data.created_on"
              ,$"right_call_data.customer_account_number"
              ,$"right_call_data.segment_handled_flag"
              ,$"right_call_data.customer_subtype"
              ,$"portals_join_customer_type".alias("customer_type")
              ,$"right_call_data.eduid"
              ,$"right_call_data.issue_description"
              ,$"right_call_data.last_handled_segment_flag"
              ,$"right_call_data.location_name"
              ,$"right_call_data.notes_txt"
              ,$"right_call_data.phone_number_from_tracker"
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
              ,$"right_call_data.account_key"
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
              $"account_key"
              ,$"account_number"
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
              $"account_key"
              ,$"account_number"
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
          venona_events_portals
            .where($"visit_account_number".isNotNull
              && $"visit_id".isNotNull)
            .withColumn("care_events_partition_date_utc",$"partition_date_utc")
            .withColumn("care_events_customer_type",upper($"portals_join_customer_type"))
            .withColumn("care_events_visit_type",upper($"visit_type"))
            .withColumn("care_events_visit_mso", $"visit_mso")
            .groupBy($"care_events_partition_date_utc",$"care_events_customer_type",$"care_events_visit_type", $"care_events_visit_mso")
            .agg(countDistinct($"visit_account_number", $"care_events_visit_mso").alias("care_events_total_acct_visits")
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
            .agg(countDistinct(when($"enhanced_account_number" === lit(false),$"account_number")).alias("call_data_total_acct_calls")
              ,countDistinct($"call_inbound_key").alias("call_data_total_calls")
              ,countDistinct(when(lower(decryptString256UDF($"account_number")) =!= "unknown" && $"enhanced_account_number" === lit(false), $"call_inbound_key")).alias("call_data_handled_acct_calls")
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
            .groupBy(
              $"account_key"
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
              ,$"account_number"
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