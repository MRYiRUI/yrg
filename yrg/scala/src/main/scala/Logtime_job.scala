import Consumerana2.spl
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Logtime_job {
  def main(args: Array[String]): Unit = {
    def catalog_log =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"}
         |}
         |}""".stripMargin

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"job":{"cf":"cf", "col":"job", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val log_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_log)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val spls = functions.udf(spl _)
    val readDF: DataFrame = log_DF
      .groupBy("global_user_id","log_time").agg(count('log_time) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('global_user_id).orderBy('count.desc))
      .where('row_num === 1)
      .drop("count", "row_num")
      .select('global_user_id,'log_time)
    val user_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val o_u: DataFrame = user_DF
      .join(readDF, user_DF.col("id") === readDF.col("global_user_id"))
      .withColumn("logtime",when(hour('log_time) between(0,7),"0点-7点")
                .when(hour('log_time) between(8,12),"8点-12点")
                .when(hour('log_time) between(13,17),"13点-17点")
                .when(hour('log_time) between(18,21),"18点-21点")
                .when(hour('log_time) between(22,24),"22点-24点")
                )
      .groupBy("job","logtime").agg(count('logtime).cast("long").cast("string")as("count"))
        .withColumn("rid", row_number() over Window.partitionBy('job).orderBy('logtime.desc))
      .withColumn("id", monotonically_increasing_id.cast("string"))
    def catalog_w =
      s"""{
         |"table":{"namespace":"default", "name":"log_job"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"rid":{"cf":"cf", "col":"rid", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"logtime":{"cf":"cf", "col":"logtime", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"string"}
         |}
         |}""".stripMargin

    o_u.write
      .option(HBaseTableCatalog.tableCatalog, catalog_w)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}