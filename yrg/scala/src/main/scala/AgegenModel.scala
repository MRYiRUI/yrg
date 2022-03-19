import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions._

//性别年龄消费能力分布图
object AgegenModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"age":{"cf":"cf", "col":"age", "type":"string"}
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
//      .agg(avg(current_date()).cast("long").cast("string").as("uvalue"),count('age))
    val result = re.groupBy('age, 'gender)
      .agg(count('age).cast("long").cast("string").as("count"),avg('用户价值).cast("string").as("uvalue"))


    val res = result.withColumn("id",monotonically_increasing_id)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"agegender"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"age":{"cf":"cf", "col":"age", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"string"},
         |"uvalue":{"cf":"cf", "col":"uvalue", "type":"string"}
         |}
         |}""".stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

  }
}