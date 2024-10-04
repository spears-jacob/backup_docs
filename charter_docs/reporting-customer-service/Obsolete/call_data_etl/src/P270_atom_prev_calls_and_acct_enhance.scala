

import org.apache.spark.sql.expressions._
import com.spectrum.crypto._
import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoders, Row, SaveMode }
import java.sql._

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

val overwritedate = spark.sql("SELECT overwritedate FROM ${env:TMP_db}.cs_call_data_overwrite").collect().toList(0).get(0).toString()
val environment = sys.env("ENVIRONMENT")

//Setting case classes to be used in the process
case class CallData(
                       call_inbound_key: String
                       ,call_id: String
                       ,call_start_date_utc: String
                       ,call_start_time_utc: String
                       ,call_end_time_utc: String
                       ,call_start_date_time_utc: String
                       ,call_end_date_time_utc: String
                       ,call_start_timestamp_utc: Double
                       ,call_end_timestamp_utc: Double
                       ,previous_call_time_utc: Option[Double]
                       ,segment_id: String
                       ,segment_number: String
                       ,segment_status_disposition: String
                       ,segment_start_time_utc: String
                       ,segment_end_time_utc: String
                       ,segment_start_date_time_utc: String
                       ,segment_end_date_time_utc: String
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
                       ,customer_account_number_encrypted: String
                       ,customer_type: Option[String]
                       ,customer_subtype: Option[String]
                       ,truck_roll_flag: Boolean
                       ,call_notes_text: String
                       ,call_resolution_description: String
                       ,call_cause_description: String
                       ,call_issue_description: String
                       ,company_code: String
                       ,service_call_tracker_id: String
                       ,created_on_date: String
                       ,created_by: String
                       ,phone_number_from_tracker: String
                       ,call_type_code: String
                       ,split_sum_description: String
                       ,location_name: String
                       ,care_center_management_name: String
                       ,agent_job_role_name: String
                       ,agent_effective_hire_date: String
                       ,agent_mso: String
                       ,edu_id: String
                       ,last_handled_segment_flag: Boolean
                       ,last_updated_date_time_stamp: String
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


// process to enhance account numbers in cs_call_data using phone number
def enhanceAccountNumbers(call_data: Dataset[CallData]): Dataset[CallData] = {
  // get all calls with valid phone numbers
  val validPhones = (
      call_data
          // filter out phone numbers that are not 10 digits or start with 0 and 1 or are unknown
          // filter out enhanced account numbers, we do not want to enhance with a previously enhanced value
          .withColumn("unencrypted_account_number", decryptString256UDF($"account_number"))
          .where(length($"phone_number_from_tracker") === 10 && !(substring($"phone_number_from_tracker",1,1).isin(0,1)) && $"enhanced_account_number" === false)
          .select($"call_inbound_key", $"phone_number_from_tracker", $"account_number", $"unencrypted_account_number", $"call_end_date_utc").distinct
      )

  // get the possible phone numbers that can be used for enhancement
  val singlePhoneAccounts = (
      validPhones
          .where(lower($"unencrypted_account_number") =!= "unknown" && !isnull($"unencrypted_account_number"))
          // count the distinct account numbers, get the last call_end_date for that phone number, and use max(account_number) to get the account number
          .groupBy($"phone_number_from_tracker")
          .agg(countDistinct($"account_number").alias("account_count"), max($"call_end_date_utc").alias("most_recent_call_date"), max($"account_number").alias("acct_num"))
          // filter to only results where the count of distinct account numbers is 1 and select out phone_number
          .where($"account_count" === 1 && $"most_recent_call_date" >= add_months(lit(overwritedate), -6))
          .select($"phone_number_from_tracker".alias("phone_number"), $"acct_num")
      )

  call_data
      .withColumn("unencrypted_account_number", coalesce(decryptString256UDF($"account_number"), lit("unknown")))
      // join enhanceable accounts back to call data using a left join
      .join(
    singlePhoneAccounts, $"phone_number" === $"phone_number_from_tracker" && lower($"unencrypted_account_number") === "unknown", "left"
  )
      // if there is a value in enhanceable accounts, update both account_number and enhanced_account_number flag
      .withColumn("account_number", coalesce($"acct_num", $"account_number"))
      .withColumn("enhanced_account_number", when(!isnull($"acct_num"), lit(true)).otherwise(lit(false)))
      .select(
        $"call_inbound_key"
        ,$"call_id"
        ,$"call_start_date_utc"
        ,$"call_start_time_utc"
        ,$"call_end_time_utc"
        ,$"call_start_date_time_utc"
        ,$"call_end_date_time_utc"
        ,$"call_start_timestamp_utc"
        ,$"call_end_timestamp_utc"
        ,$"previous_call_time_utc"
        ,$"segment_id"
        ,$"segment_number"
        ,$"segment_status_disposition"
        ,$"segment_start_time_utc"
        ,$"segment_end_time_utc"
        ,$"segment_start_date_time_utc"
        ,$"segment_end_date_time_utc"
        ,$"segment_start_timestamp_utc"
        ,$"segment_end_timestamp_utc"
        ,$"segment_duration_seconds"
        ,$"segment_duration_minutes"
        ,$"segment_handled_flag"
        ,$"customer_call_count_indicator"
        ,$"call_handled_flag"
        ,$"call_owner"
        ,$"product"
        ,$"account_number"
        ,$"customer_account_number_encrypted"
        ,$"customer_type"
        ,$"customer_subtype"
        ,$"truck_roll_flag"
        ,$"call_notes_text"
        ,$"call_resolution_description"
        ,$"call_cause_description"
        ,$"call_issue_description"
        ,$"company_code"
        ,$"service_call_tracker_id"
        ,$"created_on_date"
        ,$"created_by"
        ,$"phone_number_from_tracker"
        ,$"call_type_code"
        ,$"split_sum_description"
        ,$"location_name"
        ,$"care_center_management_name"
        ,$"agent_job_role_name"
        ,$"agent_effective_hire_date"
        ,$"agent_mso"
        ,$"edu_id"
        ,$"last_handled_segment_flag"
        ,$"last_updated_date_time_stamp"
        ,$"source"
        ,$"enhanced_account_number"
        ,$"call_end_date_utc"
      ).as[CallData]
}


// process to update previous call times in cs_call_data
def refreshPreviousCallTimes(call_data: Dataset[CallData]): Dataset[CallData] = {
  //Setting lag specification for previous call time
  // ordering by min_call_start_time and then call_inbound_key to ensure consistency in case there are multiple calls that start at the same time (small number of cases seen)
  val previous_call_time_window = Window.partitionBy($"account_number").orderBy($"min_call_start_time", $"call_inbound_key")

  val previous_call_time = (
      call_data.select($"call_inbound_key",$"account_number",$"call_start_timestamp_utc")
          .withColumn("unencrypted_account_number", decryptString256UDF($"account_number"))
          .where(lower($"unencrypted_account_number") =!= "unknown" && !isnull($"unencrypted_account_number") && $"last_handled_segment_flag" === 1)
          .groupBy($"call_inbound_key",$"account_number").agg(min($"call_start_timestamp_utc").alias("min_call_start_time"))
          .withColumn("previous_call_start_time",lag($"min_call_start_time",1).over(previous_call_time_window))
          .select($"account_number".alias("acct_num"), $"call_inbound_key".alias("previous_time_call_inbound_key"),$"previous_call_start_time")
      )

  call_data
      // join call data to previous_call_time to get previous call time
      .join(
    previous_call_time, $"call_inbound_key" === $"previous_time_call_inbound_key" && $"acct_num" === $"account_number", "left"
  )
      .select(
        $"call_inbound_key"
        ,$"call_id"
        ,$"call_start_date_utc"
        ,$"call_start_time_utc"
        ,$"call_end_time_utc"
        ,$"call_start_date_time_utc"
        ,$"call_end_date_time_utc"
        ,$"call_start_timestamp_utc"
        ,$"call_end_timestamp_utc"
        ,$"previous_call_start_time".alias("previous_call_time_utc")
        ,$"segment_id"
        ,$"segment_number"
        ,$"segment_status_disposition"
        ,$"segment_start_time_utc"
        ,$"segment_end_time_utc"
        ,$"segment_start_date_time_utc"
        ,$"segment_end_date_time_utc"
        ,$"segment_start_timestamp_utc"
        ,$"segment_end_timestamp_utc"
        ,$"segment_duration_seconds"
        ,$"segment_duration_minutes"
        ,$"segment_handled_flag"
        ,$"customer_call_count_indicator"
        ,$"call_handled_flag"
        ,$"call_owner"
        ,$"product"
        ,$"account_number"
        ,$"customer_account_number_encrypted"
        ,$"customer_type"
        ,$"customer_subtype"
        ,$"truck_roll_flag"
        ,$"call_notes_text"
        ,$"call_resolution_description"
        ,$"call_cause_description"
        ,$"call_issue_description"
        ,$"company_code"
        ,$"service_call_tracker_id"
        ,$"created_on_date"
        ,$"created_by"
        ,$"phone_number_from_tracker"
        ,$"call_type_code"
        ,$"split_sum_description"
        ,$"location_name"
        ,$"care_center_management_name"
        ,$"agent_job_role_name"
        ,$"agent_effective_hire_date"
        ,$"agent_mso"
        ,$"edu_id"
        ,$"last_handled_segment_flag"
        ,$"last_updated_date_time_stamp"
        ,$"source"
        ,$"enhanced_account_number"
        ,$"call_end_date_utc"
      ).as[CallData]
}

//pulling all data in dev.cs_call_data
//Need to rename and transform a few columns so they play nicely
val cs_call_data = (
    spark.table(s"${environment}.cs_call_data")
        .withColumn("segment_duration_seconds", $"segment_duration_seconds".cast("double"))
        .withColumn("segment_duration_minutes", $"segment_duration_minutes".cast("double"))
        .withColumn("call_start_date_time_utc", $"call_start_datetime_utc")
        .withColumn("call_end_date_time_utc", $"call_end_datetime_utc")
        .withColumn("segment_start_date_time_utc", $"segment_start_datetime_utc")
        .withColumn("segment_end_date_time_utc", $"segment_end_datetime_utc")
        .withColumn("customer_account_number_encrypted", $"customer_account_number")
        .withColumn("call_notes_text", $"notes_txt")
        .withColumn("call_resolution_description", $"resolution_description")
        .withColumn("call_cause_description", $"cause_description")
        .withColumn("call_issue_description", $"issue_description")
        .withColumn("created_on_date", $"created_on")
        .withColumn("call_type_code", $"call_type")
        .withColumn("split_sum_description", $"split_sum_desc")
        .withColumn("edu_id", $"eduid")
        .withColumn("last_updated_date_time_stamp", $"record_update_timestamp")
    )

//Calling refreshPreviousCallTimes method outlined above on all data in cs_call_data
val enhanced_accounts = enhanceAccountNumbers(cs_call_data.as[CallData])

//Calling enhanceAccountNumbers method outlined above on the data that has been updated with the previous call time
val previous_call_times = refreshPreviousCallTimes(enhanced_accounts.as[CallData]).where($"call_end_date_utc" > lit(overwritedate))

//previous_call_times.where(to_date($"call_end_date_utc") < lit("2018-10-22")).groupBy($"call_end_date_utc").agg(count($"call_end_date_utc").alias("cnt")).collect().foreach(println)

//previous_call_times.select("*").limit(10).collect().foreach(println)

// writing previous call time results and enhanced account numbers to dev.cs_call_data
previous_call_times.write.mode("overwrite").insertInto(s"${environment}.cs_call_data")


//Close spark-shell
System.exit(0)
