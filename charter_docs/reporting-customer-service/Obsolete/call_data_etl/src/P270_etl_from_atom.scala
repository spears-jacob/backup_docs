
import org.apache.spark.sql.expressions._
import com.spectrum.crypto._
import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoders, Row, SaveMode }
import java.sql._

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

val environment = sys.env("ENVIRONMENT")

val overwritedate = spark.sql("SELECT overwritedate FROM ${env:TMP_db}.cs_call_data_overwrite").collect().toList(0).get(0).toString()
println(overwritedate)

//Setting case classes to be used in the process
case class CallCareAtom(account_id_encrypted: String, account_number_encrypted: Option[String], answered_disposition_description: String, answered_disposition_id: String, call_cause_description: Option[String], call_handled_flag: Boolean, call_inbound_key: String, call_issue_description: Option[String], call_notes_text: Option[String], call_owner_name: Option[String], call_resolution_description: Option[String], call_id: String, call_segment_number: String, call_type_code: Option[String], care_center_management_name: String, legacy_corp_code: String, created_by: Option[String], created_on_date: String, customer_account_number_encrypted: String, customer_call_center_flag: Option[Boolean], edu_id: Option[String], effective_hire_date: Option[String], job_role_name: Option[String], location_name: String, last_handled_segmentation_flag: Boolean, last_updated_date_time_stamp: String, mso_agent_name: Option[String], product_lob_description: Option[String], call_segment_stop_date_time: String, call_segment_stop_date_time_in_utc: String, call_segment_start_date_time: String, call_segment_start_date_time_in_utc: String, split_sum_description: Option[String], service_call_tracker_id: Option[String], truck_roll_flag: Boolean, track_phone_number_encrypted: Option[String], unified_call_id: String, partition_date: String, call_end_date_utc: String)

case class AccountData(account_key: String, partition_date_denver: String, customer_type: String)


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

// process to get raw atom data into cs_call_data format
def processCallCareAtom(call_care_atom: Dataset[CallCareAtom], accounts: Dataset[AccountData]): Dataset[CallData] = {

  //Get first CallStart time and last CallEnd time by call inbound key for call level attributes
  val derived_call_start_and_end_times = (
      call_care_atom.groupBy($"call_inbound_key")
          .agg(min($"call_segment_start_date_time_in_utc").alias("call_start_date_time_in_utc_new")
            ,max($"call_segment_stop_date_time_in_utc").alias("call_end_date_time_in_utc_new"))
          .select($"call_inbound_key".alias("call_level_call_inbound_key")
            ,$"call_start_date_time_in_utc_new",$"call_end_date_time_in_utc_new")
      )

  //Setting ROW_NUMBER() specification for cleansing account numbers
  val rownum_partitionby = Window.partitionBy($"call_inbound_key").orderBy($"last_handled_segmentation_flag" desc, when(lower(decryptString256UDF($"account_number_encrypted")) === "unknown",0).otherwise(1) desc)
  val rownumber_by_lasthndlsegfl = row_number().over(rownum_partitionby)

  //Assigning callsaccounts DataFrame with Call Inbound Key and an accurate account number
  val callsaccounts = call_care_atom.withColumn("rownum",rownumber_by_lasthndlsegfl).select($"call_inbound_key".alias("accounts_call_inbound_key"),$"account_number_encrypted".alias("account_number")).filter("rownum = 1")

  //Getting values for Call_Inbound_Key and the new Segment_End_Date_Time
  val callinbkey_starttimes = call_care_atom.select($"call_inbound_key".alias("segment_call_inbound_key"),$"call_segment_start_date_time_in_utc").distinct()
  val segment_start_window = Window.partitionBy($"segment_call_inbound_key").orderBy($"call_segment_start_date_time_in_utc")

  //Segment End Date Time is calculated by taking the next segments Start Date time and subtracting 1 second from the value
  val segment_end_times = (
      callinbkey_starttimes.withColumn("segment_end_time", lead($"call_segment_start_date_time_in_utc",1).over(segment_start_window))
          .select($"segment_call_inbound_key"
            ,$"call_segment_start_date_time_in_utc"
            ,from_unixtime(unix_timestamp(col("segment_end_time")).minus(1), "YYYY-MM-dd HH:mm:ss").alias("segment_end_datetime")).distinct()
      )
  val cleansed_segment_end_times = (
      segment_end_times.join(call_care_atom.groupBy($"call_inbound_key".alias("segment_end_call_inbound_key")).agg(max($"call_segment_stop_date_time_in_utc").alias("segment_end_datetime_last")),$"segment_call_inbound_key" === $"segment_end_call_inbound_key", "left")
          .select($"segment_call_inbound_key"
            ,$"call_segment_start_date_time_in_utc".alias("segment_start_datetime")
            ,coalesce($"segment_end_datetime",$"segment_end_datetime_last").alias("segment_end_datetime"))
      )

  val last_handled_segment_window = Window.partitionBy($"call_inbound_key").orderBy(when($"segment_handled_flag" === 1, 1).otherwise(0) desc,$"segment_start_date_time_utc" desc)

  //Selecting out of managed and into TMP while joining to previously created datasets for cleansing
  val cca_joined = (call_care_atom
      //Join call level start and end times
      .join(
    derived_call_start_and_end_times,$"call_inbound_key" === $"call_level_call_inbound_key","left"
  )
      //Join cleansed segment end times
      .join(
    cleansed_segment_end_times,$"call_inbound_key" === $"segment_call_inbound_key" && $"call_segment_start_date_time_in_utc" === $"segment_start_datetime", "left"
  )
      //Join cleansed account
      .join(
    callsaccounts,$"call_inbound_key" === $"accounts_call_inbound_key", "left"
  )
      .withColumn("account_id", decryptStringUDF($"account_id_encrypted"))
      .withColumn("call_id", regexp_replace($"call_id","\\s+",""))
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
      .withColumn("segment_start_date_time_utc",substring($"segment_start_datetime",0,19))
      .withColumn("segment_end_date_time_utc",substring($"segment_end_datetime",0,19))
      .withColumn("segment_start_time_utc",substring($"segment_start_date_time_utc",12,8))
      .withColumn("segment_end_time_utc",substring($"segment_end_date_time_utc",12,8))
      .withColumn("segment_start_timestamp_utc",unix_timestamp(concat($"segment_start_date_time_utc",lit(" UTC")),"yyyy-MM-dd HH:mm:ss zzz") * 1000)
      .withColumn("segment_end_timestamp_utc",unix_timestamp(concat($"segment_end_date_time_utc",lit(" UTC")),"yyyy-MM-dd HH:mm:ss zzz") * 1000)
      .withColumn("segment_duration_seconds",unix_timestamp($"segment_end_date_time_utc")-unix_timestamp($"segment_start_date_time_utc"))
      .withColumn("segment_duration_minutes",$"segment_duration_seconds"/60)
      .withColumn("call_handled_flag",$"call_handled_flag".cast("int"))
      .withColumn("call_owner",coalesce($"call_owner_name",lit("Unknown")))
      .withColumn("segment_status_disposition",$"answered_disposition_description")
      .withColumn("segment_handled_flag",when($"call_handled_flag" === true && lower($"call_owner") === "customer operations" && lower($"segment_status_disposition") === "answered by an agent",1).otherwise(0))
      .withColumn("customer_call_count_indicator",lit(null))
      .withColumn("product",coalesce($"product_lob_description",lit("Unknown")))
      .withColumn("account_number",coalesce($"account_number",$"account_number_encrypted"))
      .withColumn("company_code",$"legacy_corp_code")
      .withColumn("created_on_date", date_format(to_timestamp($"created_on_date", "yyyy-MM-dd HH:mm:ss"), "MM/dd/yyyy HH:mm:ss"))
      .withColumn("agent_job_role_name",$"job_role_name")
      .withColumn("agent_effective_hire_date",when($"effective_hire_date" === "1900/01/01",null).otherwise(regexp_replace($"effective_hire_date","/","-")))
      .withColumn("phone_number_from_tracker",decryptStringUDF($"track_phone_number_encrypted"))
      .withColumn("agent_mso",$"mso_agent_name")
      .withColumn("last_handled_segment_flag",when(row_number().over(last_handled_segment_window) === 1, 1).otherwise(0))
      .withColumn("last_updated_date_time_stamp", date_format(to_timestamp($"last_updated_date_time_stamp", "yyyy-MM-dd HH:mm:ss"), "MM/dd/yyyy HH:mm:ss.SSS" ))
      .withColumn("source", lit("atom"))
      .withColumn("enhanced_account_number", lit(false))
      )

  cca_joined
      .join(
        accounts,$"account_id" === $"account_key" && $"call_end_date_denver" === $"partition_date_denver", "left"
      )
      .withColumn("customer_type", when(!isnull($"customer_type"), upper($"customer_type"))
          .otherwise(lit("UNMAPPED")))
      .withColumn("customer_subtype", lit(null))
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
        ,$"call_segment_number".alias("segment_number")
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

//Calling the processCalLCareAtom method outlined above
//Need to rename and transform a few columns so they play nicely
val cca = (processCallCareAtom(
  spark.table(s"${environment}.cs_atom_call_care_staging")
      .withColumn("call_id",$"caller_id")
      .withColumn("call_segment_number",$"call_segmentation_number")
      .withColumn("call_segment_start_date_time_in_utc",to_utc_timestamp(substring($"call_segment_start_date_time_in_est",0,19),"America/New_York"))
      //.withColumn("call_segment_start_date_time_in_est",to_timestamp(substring($"call_segment_start_date_time_in_est",0,19)))
      .withColumn("call_segment_stop_date_time_in_utc",to_utc_timestamp(substring($"call_segment_stop_date_time_in_est",0,19),"America/New_York"))
      //.withColumn("call_segment_stop_date_time_in_est",to_timestamp(substring($"call_segment_stop_date_time_in_est",0,19)))
      .withColumn("account_number_encrypted", encryptString256UDF(decryptStringUDF($"account_number_encrypted")))
      // call_end_date_utc is actually segment end date in cs_atom_call_care_staging. To make sure we don't miss any segments that may be on a different date, we pull everything from 14 days prior.
      .where(to_date($"call_end_date_utc") >= date_add(lit(overwritedate), -10))
      .as[CallCareAtom],
  spark.table("prod.quantum_atom_accounts_v")
      .select($"account_key", $"partition_date_denver", $"customer_type")
      .where(to_date($"partition_date_denver") >= date_add(lit(overwritedate), -10))
      .distinct
      .as[AccountData]
)
    .where(to_date($"call_end_date_utc") > lit(overwritedate))
    )

//cca.select($"call_inbound_key",$"call_end_date_utc").limit(10).collect().foreach(println)
//cca.groupBy($"segment_handled_flag").count().show()

// writing cca results to dev.cs_call_data
cca.write.mode("overwrite").insertInto(s"${environment}.cs_call_data")


//Close spark-shell
System.exit(0)
