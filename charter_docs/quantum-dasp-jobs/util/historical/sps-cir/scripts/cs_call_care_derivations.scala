// per Joey --
//
// 1 core, 4GB memory
// (or 2 cores, 8gb memory; or 4 cores, 16gb memory)
// spark-shell --executor-memory 4G --executor-cores 1
 //spark-shell --executor-memory 12G --executor-cores 4


import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.expressions._;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;


  //function to query tables
    val spark = {SparkSession .builder()
    .appName("cs_call_derivations")
    .config("spark.debug.maxToStringFields", 1000)
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.merge.sparkfiles", "true")
    .enableHiveSupport()
    .getOrCreate()}

    //function to clean up MSO data
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
        case _ =>  null
        }
      }
//tables that need querying
// val callDataTableName = "cs_call_care_rescom_fix"
val callDataTableName = "atom_cs_call_care_data_3"
val callRateTableName = "cs_call_in_rate_sps"
val callPriorVisitTableName = "cs_calls_with_prior_visits_sps"
// val oldCallDataTableName = "atom_cs_call_care_data_3"

//system variables
val environment = sys.env("ENVIRONMENT")
val daspDb = s"${environment}_dasp"
println(s"current environment would be ${daspDb}")
val runDate = sys.env("RUN_DATE")
val endDate = LocalDate.parse(runDate).minusDays(2)
//val overwriteDate = endDate.minusDays(1)  //use for testing
val overwriteDate = endDate.minusDays(7)
val startTime = System.nanoTime

//print  reference data for debugging
println(s"application id of Spark job is ${spark.conf.get("spark.app.id")}")
println(s"processing from ${overwriteDate} to ${endDate}.")

/////////////////////////////// Read in Call Data//////////////////////////////
val newCallData = {spark .table(s"$environment.$callDataTableName")
.where($"call_end_date_utc" >= lit(overwriteDate.toString) && $"call_end_date_utc" <= lit(endDate.toString))
.cache}

// newCallData.groupBy("account_agent_mso").count().show(10,false)
newCallData.groupBy("call_end_date_utc").count().show(20,false)

//newCallData.filter($"call_inbound_key"==="2062394323671").select($"segment_id")show(10,false)

// val oldCallData = spark .table(s"$environment.$oldCallDataTableName")
// .where($"call_end_date_utc" >= overwriteDate.toString && $"call_end_date_utc" <= endDate.toString)
// .cache
//
// oldCallData.filter($"call_inbound_key"==="2062123322148").select($"previous_call_time_utc")show(10,false)

/////////////////////////////// Read in Portals Event Data//////////////////////////////
val quantum_events_portals = (
spark.table(s"${environment}.core_quantum_events_sspp")
  .select( $"visit__account__account_number"
  ,$"visit__account__enc_account_number"
  , $"visit__visit_id".alias("visit_id")
  , $"visit__application_details__application_name"
  , $"message__category"
  , $"message__name"
  , $"received__timestamp"
  , $"partition_date_utc"
  , $"visit__account__details__mso"
  , $"visit__account__details__billing_services".alias("sps_flag")
  )
 .where( $"partition_date_utc" >= overwriteDate.toString &&
  $"partition_date_utc" <= endDate.toString &&
  $"visit__account__enc_account_number".isNotNull &&
  $"visit_id".isNotNull &&
  $"visit__account__enc_account_number" =!= "dZJho5z9MUSD35BuytdIhg==" &&
  $"visit__account__enc_account_number" =!= "Qn+T/sa8AB7Gnxhi4Wx2Xg==" &&
  $"visit__application_details__application_name".isin("SpecNet", "SMB", "MySpectrum","SpectrumForums","SpecMobile")
 )
 .withColumn("received__timestamp", ($"received__timestamp" / 1000).cast("BIGINT"))
 .withColumn("visit_type", lower($"visit__application_details__application_name"))
 .withColumn("portals_join_customer_type", when($"visit_type" === "myspectrum" || $"visit_type" === "specnet", "RESIDENTIAL")
    .otherwise(when($"visit_type" === "smb", "COMMERCIAL")
    .otherwise(when($"visit_type" === "specmobile", "SPECMOBILE")
    .otherwise(when($"visit_type" === "spectrumforums","SPECTRUMFORUMS")
      .otherwise("UNMAPPED")))))
 .withColumn("visit_mso", when($"visit__account__details__mso".isNotNull, msoTransformUDF($"visit__account__details__mso")))
.distinct
).as("portals").cache

//quantum_events_portals.filter($"visit__application_details__application_name"==="SpecMobile").show(10,false)
//quantum_events_portals.filter($"portals_join_customer_type"==="RESIDENTIAL").show(10,false)
quantum_events_portals.groupBy($"visit_type", $"partition_date_utc").count().show(10,false)


/////////////////////////////// Combine call data and event data//////////////////////////////
val cs_calls_and_events = (
 quantum_events_portals
  .join( newCallData.select(
   "encrypted_padded_account_number_256",
   "encrypted_account_number_256" ,
   "account_agent_mso" ,
   "call_inbound_key" ,
   "enhanced_account_number")
  .distinct
  .as("left_call_data")
  .where($"enhanced_account_number" === lit(false)).as("portal_atom_join")
    , $"visit__account__enc_account_number" === $"encrypted_account_number_256"
     && $"visit_mso" === $"account_agent_mso" , "inner"
  )
  .join(
     newCallData.as("right_call_data"),
     Seq("call_inbound_key", "account_agent_mso"),
     "inner"
    )
  .where(
     (($"call_start_timestamp_utc" / 1000) - $"received__timestamp") <= "86400"
       && (($"call_start_timestamp_utc" / 1000) - $"received__timestamp") >= 0)
  .distinct
  .select(
$"visit__account__enc_account_number"
   , $"visit_id"
   , $"visit_mso"
   , $"message__category"
   , $"message__name"
   , $"received__timestamp"
   , $"partition_date_utc"
   , $"visit_type"
   , $"right_call_data.agent_effective_hire_date"
   , $"right_call_data.agent_job_role_name"
   , $"right_call_data.encrypted_account_number_256"
   , $"right_call_data.encrypted_padded_account_number_256"
   , $"right_call_data.call_end_date_utc"
   , $"right_call_data.call_end_datetime_utc"
   , $"right_call_data.call_end_time_utc"
   , $"right_call_data.call_end_timestamp_utc"
   , $"call_id"
   , $"call_inbound_key"
   , $"right_call_data.call_start_date_utc"
   , $"right_call_data.call_start_datetime_utc"
   , $"right_call_data.call_start_time_utc"
   , $"right_call_data.call_start_timestamp_utc"
   , $"right_call_data.call_type"
   , $"right_call_data.care_center_management_name"
   , $"right_call_data.cause_description"
   , $"right_call_data.company_code"
   , $"right_call_data.encrypted_created_by_256"
   , $"right_call_data.created_on"
   , $"encrypted_customer_account_number_256"
   , $"right_call_data.segment_handled_flag"
   , $"right_call_data.customer_subtype"
   , $"portals_join_customer_type".alias("customer_type")
   , $"right_call_data.eduid"
   , $"right_call_data.issue_description"
   , $"right_call_data.last_handled_segment_flag"
   , $"right_call_data.location_name"
   , $"encrypted_notes_txt_256"
   , $"encrypted_phone_number_from_tracker_256"
   , $"right_call_data.previous_call_time_utc"
   , $"right_call_data.product"
   , $"right_call_data.record_update_timestamp"
   , $"right_call_data.resolution_description"
   , $"right_call_data.segment_duration_minutes"
   , $"right_call_data.segment_duration_seconds"
   , $"right_call_data.segment_end_datetime_utc"
   , $"right_call_data.segment_end_time_utc"
   , $"right_call_data.segment_end_timestamp_utc"
   , $"right_call_data.segment_number"
   , $"right_call_data.segment_start_datetime_utc"
   , $"right_call_data.segment_start_time_utc"
   , $"right_call_data.segment_start_timestamp_utc"
   , $"right_call_data.service_call_tracker_id"
   , $"right_call_data.split_sum_desc"
   , $"right_call_data.truck_roll_flag"
   , $"encrypted_account_key_256"
   , $"portal_atom_join.account_agent_mso"
   , $"right_call_data.enhanced_mso"
   , $"right_call_data.segment_id"
   , $"sps_flag")
 ).distinct.as("call_and_event").cache

   // cs_calls_and_events.filter($"segment_id"==="2062394323671-83887751-1").select($"call_inbound_key",$"segment_id",$"segment_handled_flag",$"customer_type",$"enhanced_mso",$"visit_id",$"received__timestamp",$"segment_start_timestamp_utc",$"previous_call_time_utc").show(30,false)
   // cs_calls_and_events.filter($"visit_mso"==="CHR").select($"visit_id").show(10,false)
cs_calls_and_events.groupBy("visit_type").count().show(10,false)



/////////////////////////////// Further tweaking on that calls-events join //////////////////////////////
   val steve_calls_with_prior_visit = (
    cs_calls_and_events
     .where($"segment_handled_flag" === lit(true))
     .groupBy(
       $"encrypted_account_key_256"
       , $"encrypted_padded_account_number_256"
       , $"visit_type"
       , $"call_inbound_key"
       , $"customer_type"
       , $"customer_subtype"
       , $"visit_mso"
       , $"product"
       , $"issue_description"
       , $"cause_description"
        , $"resolution_description"
        , $"call_start_timestamp_utc"
        , $"call_end_date_utc"
        , $"previous_call_time_utc"
        , $"account_agent_mso"
        , $"enhanced_mso"
        , $"segment_id"
        , $"truck_roll_flag"
        , $"visit_id"
        , $"sps_flag"
        )
        .agg(min($"received__timestamp").alias("visitStart"), max($"received__timestamp").alias("visitEnd"))
        .where($"visitStart" < ($"call_start_timestamp_utc" / 1000)
          && ($"visitStart" > ($"previous_call_time_utc" / 1000) || $"previous_call_time_utc".isNull))
        .withColumn("call_start_div", ($"call_start_timestamp_utc" / 1000))
        .select(
          $"encrypted_account_key_256"
          , $"encrypted_padded_account_number_256"
          , $"call_inbound_key"
          , $"customer_type"
          , $"customer_subtype"
          , $"visit_mso"
          , $"product"
          , $"issue_description"
          , $"cause_description"
          , $"resolution_description"
          , $"call_start_div"
          , $"visit_type"
          , $"visitStart"
          , $"call_end_date_utc"
          , $"account_agent_mso"
          , $"enhanced_mso"
          , $"segment_id"
          , $"truck_roll_flag"
          , $"visit_id"
          , $"sps_flag"
          )
        ).as("prior_visit").cache

 //steve_calls_with_prior_visit.filter($"call_inbound_key"==="2062123322148").select($"call_inbound_key",$"segment_id",$"customer_type",$"enhanced_mso",$"visit_id").show(10,false)
 //steve_calls_with_prior_visit.filter($"visit_mso"==="CHR").select($"call_inbound_key",$"segment_id",$"customer_type",$"enhanced_mso",$"visit_id").show(10,false)
steve_calls_with_prior_visit.groupBy("visit_type","call_end_date_utc").count().sort("call_end_date_utc").show(100,false)

///////////////////////////////Tweak call-events join to make cs_calls_with_prior_visits//////////////////////////////
val steve_calls_with_visits = (
 steve_calls_with_prior_visit
 .withColumn("call_date", $"call_end_date_utc")
 .withColumn("account_number", $"encrypted_padded_account_number_256")
 .withColumn("account_key", $"encrypted_account_key_256")
 .groupBy(
   $"account_key"
   , $"account_number"
   , $"customer_type"
   , $"customer_subtype"
   , $"call_inbound_key"
   , $"product"
   , $"visit_mso"
   , $"visit_type"
   , $"issue_description"
   , $"cause_description"
   , $"resolution_description"
   , $"call_date"
   , $"account_agent_mso"
   , $"enhanced_mso"
   , $"segment_id"
   , $"truck_roll_flag"
   , $"visit_id"
   , $"sps_flag"
 )
 .agg(((min($"call_start_div") - max($"visitstart")) / 60).alias("minutes_to_call"))
 .select(
   $"account_key"
   , $"account_number"
   , $"customer_type"
   , $"customer_subtype"
   , $"call_inbound_key"
   , $"product"
   , $"visit_mso".alias("agent_mso")
   , $"visit_type"
   , $"issue_description"
   , $"cause_description"
   , $"resolution_description"
   , $"minutes_to_call"
   , $"account_agent_mso"
   , $"enhanced_mso"
   , $"segment_id"
   , $"truck_roll_flag"
   , $"call_date"
   , $"visit_id"
   , $"sps_flag"
 )
)
        steve_calls_with_visits.show(10,false)

steve_calls_with_visits.createOrReplaceTempView("steve_calls_with_visits")

/////////////////////////////// Write cs_calls_with_prior_visits//////////////////////////////
spark.sql(
  s"""INSERT OVERWRITE TABLE ${daspDb}.${callPriorVisitTableName} PARTITION(call_date)
     |SELECT account_key,
     |account_number,
     |customer_type,
     |customer_subtype,
     |call_inbound_key,
     |product,
     |agent_mso,
     |visit_type,
     |issue_description,
     |cause_description,
     |resolution_description,
     |minutes_to_call,
     |account_agent_mso,
     |enhanced_mso,
     |segment_id,
     |truck_roll_flag,
     |visit_id,
     |sps_flag,
     |call_date FROM steve_calls_with_visits DISTRIBUTE BY call_date""".stripMargin
  )

/////////////////////////////// Create the three elements of Call-in Rate //////////////////////////////
 val cs_visit_rate_steve_care_events = (
   quantum_events_portals
    .where($"visit__account__enc_account_number".isNotNull && $"visit_id".isNotNull)
    .withColumn("care_events_partition_date_utc", $"partition_date_utc")
    .withColumn("care_events_customer_type", upper($"portals_join_customer_type"))
    .withColumn("care_events_visit_type", upper($"visit_type"))
    .withColumn("care_events_visit_mso", $"visit_mso")
    .groupBy($"care_events_partition_date_utc", $"care_events_customer_type", $"care_events_visit_type", $"care_events_visit_mso", $"sps_flag")
    .agg(countDistinct($"visit__account__enc_account_number", $"care_events_visit_mso").alias("care_events_distinct_visit_accts")
     , countDistinct($"visit_id").alias("care_events_authenticated_visits") ) ).as("cs_visit_rate_care")

                 //cs_visit_rate_steve_care_events.show(10,false)


 val cs_visit_rate_call_data = (
   newCallData
    .withColumn("call_data_call_date", $"call_end_date_utc")
    .withColumn("call_data_agent_mso", $"account_agent_mso")
    .withColumn("call_data_customer_type", upper($"customer_type"))
    .where($"segment_handled_flag" === lit(true)
      && $"enhanced_account_number" === lit(false))
    .groupBy($"call_data_call_date", $"call_data_agent_mso", $"call_data_customer_type")
    .agg(countDistinct(when($"enhanced_account_number" === lit(false)
          , $"encrypted_padded_account_number_256")).alias("call_data_distinct_call_accts")
      , countDistinct($"call_inbound_key").alias("call_data_handled_calls")
      , countDistinct(when($"encrypted_padded_account_number_256" =!= "+yzQ1eS5iRmWnflCmvNlSg=="
       && $"enhanced_account_number" === lit(false), $"call_inbound_key")).alias("call_data_validated_calls")
                     ) ).as("cs_visit_rate")

         //cs_visit_rate_call_data.show(10,false)

val cs_visit_calls_with_prior_visit = (
 steve_calls_with_prior_visit
  .withColumn("prior_visit_call_date", $"call_end_date_utc")
  .withColumn("prior_visit_visit_type", upper($"visit_type"))
  .withColumn("prior_visit_mso", $"visit_mso")
  .withColumn("prior_visit_customer_type", upper($"customer_type"))
  .groupBy($"prior_visit_call_date", $"prior_visit_mso", $"prior_visit_customer_type", $"prior_visit_visit_type", $"sps_flag")
  .agg(countDistinct($"call_inbound_key").alias("prior_visit_calls_with_visit"))
         ).as("cs_visit_prior_visit")

                 //cs_visit_calls_with_prior_visit.show(10,false)


/////////////////////////////// Combine those three elements in to cs_call_in_rate//////////////////////////////
val steve_visit_rate_4calls = (
 cs_visit_rate_steve_care_events
  .join(cs_visit_rate_call_data,
    $"care_events_partition_date_utc" === $"call_data_call_date"
    && upper(trim($"care_events_customer_type")) === upper(trim($"call_data_customer_type"))
    && $"care_events_visit_mso" === $"call_data_agent_mso"
    , "outer")
  .join(cs_visit_calls_with_prior_visit.withColumnRenamed("sps_flag","sps_flag2"),
    $"care_events_partition_date_utc" === $"prior_visit_call_date"
    && upper(trim($"care_events_visit_type")) === upper(trim($"prior_visit_visit_type"))
    && $"care_events_visit_mso" === $"prior_visit_mso"
    && $"sps_flag" === $"sps_flag2"
    && upper(trim($"care_events_customer_type")) === upper(trim($"prior_visit_customer_type"))
    , "outer")
   .withColumn("call_date", coalesce($"care_events_partition_date_utc", $"call_data_call_date", $"prior_visit_call_date"))
   .withColumn("acct_agent_mso", upper(coalesce($"care_events_visit_mso", $"prior_visit_mso", $"call_data_agent_mso", lit("UNMAPPED"))))
   .withColumn("visit_type", coalesce($"care_events_visit_type", $"prior_visit_visit_type", lit("UNKNOWN")))
   .withColumn("customer_type", coalesce($"care_events_customer_type", $"prior_visit_customer_type", $"call_data_customer_type", lit("UNMAPPED")))
   .withColumn("calls_with_visit", coalesce($"prior_visit_calls_with_visit", lit(0)))
   .withColumn("validated_calls", coalesce($"call_data_validated_calls", lit(0)))
   .withColumn("distinct_call_accts", coalesce($"call_data_distinct_call_accts", lit(0)))
   .withColumn("handled_calls", coalesce($"call_data_handled_calls", lit(0)))
   .withColumn("distinct_visit_accts", coalesce($"care_events_distinct_visit_accts", lit(0)))
   .withColumn("authenticated_visits", coalesce($"care_events_authenticated_visits", lit(0)))
   .select($"acct_agent_mso", $"visit_type", $"customer_type" , $"calls_with_visit", $"validated_calls", $"distinct_call_accts"
     , $"handled_calls", $"distinct_visit_accts", $"authenticated_visits", $"sps_flag", $"call_date"
     )
     .where($"call_date" >= overwriteDate.toString
        && $"call_date" <= endDate.toString)
      )

/////////////////////////////// Write cs_call_in_rate//////////////////////////////
steve_visit_rate_4calls.sort(col("call_date").desc,col("visit_type"),col("acct_agent_mso")).show(20,false)
// steve_visit_rate_4calls.filter($"visit_type"==="SPECMOBILE").show(10,false)
steve_visit_rate_4calls.createOrReplaceTempView("rate4calls")
println(s"writing ${daspDb}.${callRateTableName} for data between ${overwriteDate} and ${endDate}")
spark.sql(
   s"""INSERT OVERWRITE TABLE ${daspDb}.${callRateTableName} PARTITION(call_date)
     | SELECT acct_agent_mso,
     | visit_type,
     | customer_type,
     | calls_with_visit,
     | validated_calls,
     | distinct_call_accts,
     | handled_calls,
     | distinct_visit_accts,
     | authenticated_visits,
     | sps_flag,
     | call_date FROM rate4calls DISTRIBUTE BY call_date""".stripMargin
   )

println(s"  - [ elapsed time = ${(System.nanoTime - startTime) / 1e9} seconds for ${endDate}]")

//close spark-shell
System.exit(0)
