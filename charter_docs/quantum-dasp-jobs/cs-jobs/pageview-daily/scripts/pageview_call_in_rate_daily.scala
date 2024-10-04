import org.apache.spark.sql.expressions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}
import java.sql._

import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.functions._

import spark.implicits._

//Logger.getLogger("org").setLevel(Level.OFF)
//Logger.getLogger("akka").setLevel(Level.OFF)

try {

  val spark = SparkSession
    .builder()
    .appName("cs_call_derivations")
    .config("spark.debug.maxToStringFields", 1000)
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  //spark.conf.set("spark.dynamicAllocation.enabled", false)
  spark.conf.set("spark.sql.hive.convertMetastoreOrc", true)
  spark.conf.set("spark.sql.orc.char.enabled", true)
  spark.conf.set("spark.sql.orc.enabled", true)
  spark.conf.set("spark.sql.orc.filterPushdown", true)
  spark.conf.set("spark.sql.orc.impl", "native")
  spark.conf.set("spark.sql.session.timeZone", "America/Denver")
  spark.conf.set("spark.sql.shuffle.partitions", 200)

  println(s"application id of Spark job is ${spark.conf.get("spark.app.id")}")

  val environment = sys.env("ENVIRONMENT")
  val runDate = sys.env("RUN_DATE")
  val dasp_db = sys.env("DASP_db")
  val ratePageViewTableName = "cs_page_view_call_in_rate"
  val rateSitePageViewTableName = "cs_site_section_call_in_rate"

  def daysBetween(from: LocalDate, to: LocalDate): Seq[LocalDate] = (
    Iterator
      .iterate(from)(_.plusDays(1))
      .take(ChronoUnit.DAYS.between(from, to.plusDays(1)).toInt)
      .toSeq
    )

    def groupURLs (url:String) : String = {
      if(url != null)
      {
        val servicesPattern = """services\/(internet|voice|tv)\/.*,""".r
        val trackApptsPattern = """track-appts\/.*""".r
        val servicesResultPattern = """services\/(internet|voice|tv)""".r

        val serviceMatchResult = servicesPattern.findFirstIn(url)
        val trackApptsMatchResult = trackApptsPattern.findFirstIn(url)

        serviceMatchResult match {
          case Some(s) => servicesResultPattern.findFirstIn(url)
                                               .foreach{ s => return s }
          case None =>
        }

        trackApptsMatchResult match {
          case Some(s) => return "track-appts"
          case None => return url
        }
      }
      else { return null }
    }

  val groupURlsUDF = udf(groupURLs(_:String))

  object udfs extends Serializable {
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
          case "NONE" => "UNKNOWN"
          case "UNKNOWN" => "UNKNOWN"
          case unknown: String => unknown
          case _ => "UNKNOWN"
        }
    }

    val bool2intUDF = udf { b: Boolean => if (b) 1 else 0 }
  }

  // backfill code
  //val dates: Seq[LocalDate] =  daysBetween(LocalDate.parse("2019-01-30"), LocalDate.parse("2019-02-05")) ++ daysBetween(LocalDate.parse("2019-03-07"), LocalDate.parse("2019-04-09"))
  val dates = Seq(LocalDate.parse(runDate).minusDays(1))

  dates.foreach(date => println(date.toString))

  dates.foreach {
    processDate =>
      val startTime = System.nanoTime

      val endDate = processDate.toString

      // backfill code
      //val overwriteDate = processDate.toString
      val overwriteDate = processDate.minusDays(7).toString

      println(s"processing from ${overwriteDate} to ${endDate}.")

      val pageViews = {
        spark.table(s"${environment}.core_quantum_events_sspp")
          .where(
            $"partition_date_utc" <= endDate
              && $"partition_date_utc" >= lit(LocalDate.parse(overwriteDate).minusDays(2).toString)
              && $"message__category" === "navigation"
              && $"message__name" === "pageView"
              // && $"visit__account__account_number" =!= "7FbKtybuOWU4/Q0SRInbHA=="
              // && $"visit__account__account_number" =!= "GSWNkZXIfDPD6x25Na3i8g=="
              && $"visit__account__enc_account_number" =!= "dZJho5z9MUSD35BuytdIhg==" //AMPR: These have been changed to the new encryption scheme
              && $"visit__account__enc_account_number" =!= "Qn+T/sa8AB7Gnxhi4Wx2Xg=="
              && $"visit__application_details__application_name".isin("SpecNet", "SMB", "MySpectrum", "IDManagement", "PrivacyMicrosite","SpecMobile")
          )
          .select(
            // TODO: (ghmulli) still think this should be message timestamp
            $"received__timestamp",
            $"visit__account__enc_account_number",
            $"state__view__current_page__app_section".alias("current_app_section"),
            $"state__view__current_page__page_url",
            $"state__view__current_page__page_name",
            upper($"visit__application_details__application_name").alias("application_name"),
            $"visit__account__details__mso",
            $"visit__visit_id".alias("visit_id"),
            $"received__timestamp".alias("current_page_received_timestamp"),
            // // NOTE: the idea here is to munge the event account number and transcrypt so that this can be joined directly
            // //       against the Accounts Atom
            // udfs.encrypt256(
            //   regexp_replace(
            //     substring_index(udfs.decrypt($"visit__account__account_number"), "-", -2),
            //     "-",
            //     ""
            //   )
            // ).as("encrypted_account_number"),
            $"partition_date_utc"
          )
          .withColumn("legacy_company", when($"visit__account__details__mso".isNotNull, udfs.msoTransformUDF($"visit__account__details__mso")))
          .withColumn("source_system", lit("QUANTUM"))
          .withColumn("encrypted_account_number_256", $"visit__account__enc_account_number")
          .withColumn("page_view_url_no_parameters", when($"state__view__current_page__page_url".isNotNull,
            regexp_replace(
              rtrim(
                when(
                  instr($"state__view__current_page__page_url", "https://www.spectrum.net/user-preferences") === 1,
                  regexp_replace($"state__view__current_page__page_url", "(\\?|#).*$", "")
                )
                  .otherwise(
                    regexp_replace($"state__view__current_page__page_url", "\\?.*$", "")
                  ),
                "/"
              ), "(https://www.spectrum.net/|https://www.spectrumbusiness.net/)", ""
            )
          ).otherwise($"state__view__current_page__page_name")
          )
        .withColumn("page_view_url", groupURlsUDF(col("page_view_url_no_parameters")))
        // might need to separate this chunk
        //          .groupBy("state__view__current_page__app_section", "page_view_url")
        //          .count
        //          .sort($"count".desc)
      }

      val callData = {
        spark.table(s"${environment}.atom_cs_call_care_data_3")
          .where(
            $"enhanced_account_number" === false
              && $"call_end_date_utc" <= endDate
              && $"call_end_date_utc" >= overwriteDate
          )
          .select(
            $"call_inbound_key",
            $"call_start_timestamp_utc",
            // $"account_number".alias("encrypted_account_number"),
            $"encrypted_padded_account_number_256",
            $"encrypted_account_number_256",
            $"segment_handled_flag".alias("segment_handled_int"),
            $"account_agent_mso"
          )
          .withColumn("legacy_company", when($"account_agent_mso".isNotNull, udfs.msoTransformUDF($"account_agent_mso")))
          .groupBy("encrypted_account_number_256", "call_inbound_key", "call_start_timestamp_utc", "legacy_company")
          .agg(sum("segment_handled_int").alias("segment_handled_count"))
          .withColumn("call_contains_handled_segment", when($"segment_handled_count" > 0, true).otherwise(false))
          .select(
            $"call_inbound_key",
            $"call_start_timestamp_utc",
            $"encrypted_account_number_256",
            $"legacy_company",
            $"call_contains_handled_segment"
          )
      }


      val pageviewsAndCalls = {
        pageViews
          .join(
            callData,
            Seq("encrypted_account_number_256", "legacy_company"),
            "left"
          )
          .select(
            "encrypted_account_number_256",
            "legacy_company",
            "page_view_url",
            "current_app_section",
            "call_inbound_key",
            "call_contains_handled_segment",
            "call_start_timestamp_utc",
            "current_page_received_timestamp",
            "source_system",
            "partition_date_utc",
            "application_name",
            "visit_id"
          )
      }.cache

      val visitCallsInsideTime = {
        pageviewsAndCalls
          .select(
            "application_name",
            "page_view_url",
            "current_app_section",
            "partition_date_utc",
            "source_system",
            "legacy_company",
            "visit_id"
          )
          .where(
            $"call_start_timestamp_utc".isNotNull
              && $"page_view_url".isNotNull
              && $"call_start_timestamp_utc" - $"current_page_received_timestamp" >= 0
              && $"call_start_timestamp_utc" - $"current_page_received_timestamp" <= 86400000
          )
          .withColumn("counter", lit("counter"))
      }.cache

      val visitCalls = {
        visitCallsInsideTime
          .groupBy(
            "application_name",
            "page_view_url",
            "current_app_section",
            "partition_date_utc",
            "source_system",
            "legacy_company"
          )
          .agg(
            count($"counter").alias("count_of_pageviews_with_calls"),
            countDistinct($"visit_id").alias("count_of_distinct_visits_with_calls")
          )
      }

      val totalVisits = {
        pageViews
          .withColumn("counter", lit("counter"))
          .groupBy(
            "application_name",
            "page_view_url",
            "current_app_section",
            "partition_date_utc",
            "source_system",
            "legacy_company"
          )
          .agg(
            count($"counter").alias("total_pageviews"),
            countDistinct($"visit_id").alias("total_distinct_visits")
          )
      }

      val pageviewCallInRates = {
        visitCalls
          .join(
            totalVisits,
            Seq("application_name", "page_view_url", "partition_date_utc", "legacy_company", "current_app_section", "source_system"),
            "outer"
          )
          .select(
            $"application_name",
            $"page_view_url".alias("current_page_name"),
            $"current_app_section",
            coalesce($"count_of_pageviews_with_calls", lit(0)).alias("count_of_pageviews_with_calls"),
            $"total_pageviews",
            coalesce($"count_of_distinct_visits_with_calls", lit(0)).alias("count_of_distinct_visits_with_calls"),
            $"total_distinct_visits",
            bround(($"count_of_distinct_visits_with_calls" / $"total_distinct_visits"), 4).alias("call_in_rate"), //as decimal(12,4)) call_in_rate
            $"legacy_company".alias("mso"),
            $"source_system",
            $"partition_date_utc"
          ).where($"partition_date_utc" >= overwriteDate
          && $"partition_date_utc" <= endDate)
      }.createOrReplaceTempView("tmp_pageview_call_in_rates")


      spark.sql(
        s"""INSERT OVERWRITE TABLE ${dasp_db}.${ratePageViewTableName} PARTITION(partition_date_utc)
           |SELECT application_name,
           |  current_page_name,
           |  current_app_section,
           |  count_of_pageviews_with_calls,
           |  total_pageviews,
           |  count_of_distinct_visits_with_calls,
           |  total_distinct_visits,
           |  call_in_rate,
           |  mso,
           |  source_system,
           |  partition_date_utc FROM tmp_pageview_call_in_rates DISTRIBUTE BY partition_date_utc""".stripMargin
      )

      val visitsCallsSiteSection = {
        visitCallsInsideTime
          .groupBy(
            "application_name",
            "current_app_section",
            "partition_date_utc",
            "source_system",
            "legacy_company"
          )
          .agg(
            count($"counter").alias("sectionviews_with_calls"),
            countDistinct($"visit_id").alias("distinct_visits_with_calls")
          )
      }

      val visitsPerSiteSectionTotal = {
        pageViews
          .withColumn("counter", lit("counter"))
          .groupBy(
            "application_name",
            "current_app_section",
            "partition_date_utc",
            "source_system",
            "legacy_company"
          )
          .agg(
            count($"counter").alias("total_sectionviews"),
            countDistinct($"visit_id").alias("total_distinct_visits")
          )
      }

      val siteSectionCallInRate = {
        visitsCallsSiteSection
          .join(
            visitsPerSiteSectionTotal,
            Seq("application_name",
              "current_app_section",
              "partition_date_utc",
              "legacy_company",
              "source_system"),
            "outer"
          )
          .withColumn("count_of_sectionviews_with_calls", when($"sectionviews_with_calls".isNotNull, $"sectionviews_with_calls").otherwise(0))
          .withColumn("count_of_distinct_visits_with_calls", when($"distinct_visits_with_calls".isNotNull, $"distinct_visits_with_calls").otherwise(0))
          .select(
            $"application_name",
            $"current_app_section",
            $"legacy_company".alias("mso"),
            $"partition_date_utc",
            $"source_system",
            bround(($"count_of_distinct_visits_with_calls" / $"total_distinct_visits"), 4).alias("call_in_rate"),
            $"total_sectionviews",
            $"total_distinct_visits",
            $"count_of_sectionviews_with_calls",
            $"count_of_distinct_visits_with_calls"
          ).where($"partition_date_utc" >= overwriteDate
          && $"partition_date_utc" <= endDate)
      }.createOrReplaceTempView("tmp_sitesection_call_in_rate")

      spark.sql(
        s"""INSERT OVERWRITE TABLE ${dasp_db}.${rateSitePageViewTableName} PARTITION(partition_date_utc)
           |SELECT application_name,
           | current_app_section,
           | mso,
           | call_in_rate,
           | source_system,
           | total_sectionviews,
           | total_distinct_visits,
           | count_of_sectionviews_with_calls,
           | count_of_distinct_visits_with_calls,
           | partition_date_utc FROM tmp_sitesection_call_in_rate DISTRIBUTE BY partition_date_utc""".stripMargin
      )

      println(s"  - [ elapsed time = ${(System.nanoTime - startTime) / 1e9} seconds for ${endDate}]")
  }

} catch {
  case e: Exception =>
    println(s"exception occurred - \n ${e.getCause}")
    e.printStackTrace()
    System.exit(1)
}
//Close spark-shell
System.exit(0)
