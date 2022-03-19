import UserModel.spl
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession, functions}



//用户称号
object UsertitModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"}
         |}
         |}""".stripMargin

    def catalog2 =
      s"""{
         |"table":{"namespace":"default", "name":"consumer_anas"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"省钱能手":{"cf":"cf", "col":"省钱能手", "type":"string"}
         |}
         |}""".stripMargin

    def catalog3 =
      s"""{
         |"table":{"namespace":"default", "name":"consumer_ana1"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"有券必买":{"cf":"cf", "col":"有券必买", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val spls = functions.udf(spl _)
      val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    val readDF1: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    val readDF2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog3)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val re = readDF.join(readDF1,readDF.col("id") === readDF1.col("id")).drop(readDF1.col("id"))
    val re2 = re.join(readDF2,re.col("id") === readDF2.col("id")).drop(readDF2.col("id"))
    val result = re2.withColumn("memberId",'id.cast("long").cast("string"))

    def catalog_write =
      s"""{
         |"table":{"namespace":"default", "name":"usertitle"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"省钱能手":{"cf":"cf", "col":"省钱能手", "type":"string"},
         |"有券必买":{"cf":"cf", "col":"有券必买", "type":"string"}
         |}
         |}""".stripMargin
    result.write
      .option(HBaseTableCatalog.tableCatalog, catalog_write)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

  }
}
