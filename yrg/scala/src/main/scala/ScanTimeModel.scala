import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

//浏览时段
object ScanTimeModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

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

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_log)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
val result = readDF.select('log_time,'global_user_id,
          when(hour('log_time) between(0,7),"0点-7点")
            .when(hour('log_time) between(8,12),"8点-12点")
            .when(hour('log_time) between(13,17),"13点-17点")
            .when(hour('log_time) between(18,21),"18点-21点")
            .when(hour('log_time) between(22,24),"22点-24点")
            .as("浏览时段"))
  .groupBy('global_user_id,'浏览时段).agg(count('浏览时段).cast("long").cast("string").as("count"))
    .withColumn("meiyong",dense_rank() over Window.partitionBy('global_user_id).orderBy('浏览时段.desc))
  .withColumn("id", monotonically_increasing_id.cast("string"))
    def catalog_write =
      s"""{
         |"table":{"namespace":"default", "name":"scantime"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"浏览时段":{"cf":"cf", "col":"浏览时段", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"string"},
         |"meiyong":{"cf":"cf", "col":"meiyong", "type":"string"}
         |}
         |}""".stripMargin
    result.write
          .option(HBaseTableCatalog.tableCatalog, catalog_write)
          .option(HBaseTableCatalog.newTable, "5")
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()

  }
}