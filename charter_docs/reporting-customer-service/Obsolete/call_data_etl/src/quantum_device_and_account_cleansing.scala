import org.apache.spark.sql.expressions._
import com.spectrum.crypto._

//Setting up the encrypt and decrypt functions
val encryptString: (String => String) = Aes.encrypt(_)
val encryptStringUDF = udf(encryptString)
val encryptString256: (String => String) = Aes.encrypt256(_)
val encryptString256UDF = udf(encryptString256)

val decryptString: (String => String) = Aes.decrypt(_)
val decryptStringUDF = udf(decryptString)
val decryptString256: (String => String) = Aes.decrypt256(_)
val decryptString256UDF = udf(decryptString256)

//Get Columns and search for Device
//spark.sql("desc prod.asp_v_venona_events_portals").filter($"col_name".contains("device")).collect().foreach(println)
//spark.sql("desc prod.asp_v_venona_events_portals").orderBy($"col_name").collect().foreach(println)


//Defining quantum_events DataFrame
val quantum_events = spark.table("prod.asp_v_venona_events_portals")

//val visits_accounts = quantum_events.groupBy($"visit__device__uuid",decryptString256UDF($"visit__account__account_number_aes_256").alias("account_number")).agg(max($"partition_date_utc"))

//val pre_visits_accounts = quantum_events.select($"visit__device__uuid",decryptString256UDF($"visit__account__account_number_aes_256").alias("account_number"),$"partition_date_utc").filter($"account_number" =!= "pending_login").filter($"partition_date_utc" === "2018-09-02").distinct()
//val visits_accounts = pre_visits_accounts.groupBy($"visit__device__uuid",$"account_number").agg(max($"partition_date_utc").alias("max_partition_date_utc")).select($"visit__device__uuid",$"account_number",$"max_partition_date_utc")

val previous_uuid = Window.partitionBy($"visit__device__uuid").orderBy(when(decryptString256UDF($"visit__account__account_number_aes_256") === "pending_login",0).otherwise(1) desc,$"received__timestamp" desc)
val deviceid_accounts = { quantum_events.withColumn("previous_account_number",lag(decryptString256UDF($"visit__account__account_number_aes_256"),1).over(previous_uuid))
    .select($"visit__visit_id".alias("visit_id")
      ,$"visit__device__uuid".alias("device_id")
      ,coalesce(decryptString256UDF($"visit__account__account_number_aes_256"),lit("pending_login")).alias("account_number")
      ,coalesce($"previous_account_number",Seq("pending_login")).alias("previous_account_number")
      ,$"partition_date_utc"
    )
}

deviceid_accounts.select($"visit_id",$"device_id",$"account_number",$"previous_account_number").filter($"previous_account_number" =!= "pending_login").createOrReplaceTempView("visitsaccounts")
spark.sql("drop table if exists dev.cs_visits_accounts")
spark.sql("create table dev.cs_visits_accounts AS select * from visitsaccounts")

//$"account_number" === "pending_login" &&




//received__timestamp
