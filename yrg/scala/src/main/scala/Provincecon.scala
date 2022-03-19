import UserModel.spl
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

//省份与消费能力
object Provincecon {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"province":{"cf":"cf", "col":"province", "type":"string"}
         |}
         |}""".stripMargin

        def catalog2 =
          s"""{
             |"table":{"namespace":"default", "name":"consumer_rfm"},
             |"rowkey":"memberId",
             |"columns":{
             |"memberId":{"cf":"rowkey", "col":"memberId", "type":"string"},
             |"用户价值":{"cf":"cf", "col":"用户价值", "type":"string"}
             |}
             |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val readDF2: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val re = readDF.join(readDF2,readDF.col("id") === readDF2.col("memberId"))
      .groupBy('province).agg(avg('用户价值).cast("long").cast("string").as("uvalue")).withColumn("id",monotonically_increasing_id)

        def catalogWrite =
          s"""{
             |"table":{"namespace":"default", "name":"provincerf"},
             |"rowkey":"id",
             |"columns":{
             |"id":{"cf":"rowkey", "col":"id", "type":"string"},
             |"province":{"cf":"cf", "col":"province", "type":"string"},
             |"uvalue":{"cf":"cf", "col":"uvalue", "type":"string"}
             |}
             |}""".stripMargin
        re.write
          .option(HBaseTableCatalog.tableCatalog, catalogWrite)
          .option(HBaseTableCatalog.newTable, "5")
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()

  }

}
