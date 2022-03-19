import Consumerana2.spl
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._


//每个工作花钱
object Amount_job {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |"orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"}
         |}
         |}""".stripMargin

    def catalog_u =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"store":{"cf":"cf", "col":"store", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val order_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val spls = functions.udf(spl _)
    val readDF: DataFrame = order_DF
        .where('orderStatus==="202")
      .withColumn("memberId", spls('memberId).cast("long").cast("string"))
      .groupBy("memberId").agg(sum('orderAmount)as("totalordder"))
    val user_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_u)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val o_u:DataFrame = user_DF.join(readDF,user_DF.col("id")===readDF.col("memberId"))
     .groupBy('job).agg(sum('totalordder).cast("long").cast("string")as("Amount_job"))
      .withColumn("id", monotonically_increasing_id.cast("string"))
    def catalog_w =
      s"""{
         |"table":{"namespace":"default", "name":"Amount_job"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"Amount_job":{"cf":"cf", "col":"Amount_job", "type":"string"}
         |}
         |}""".stripMargin
    o_u.write
      .option(HBaseTableCatalog.tableCatalog, catalog_w)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}