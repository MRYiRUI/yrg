import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


//消费周期及购买物品
object CycleModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"}
         |}
         |}""".stripMargin

    def catalog2 =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"}
         |}
         |}""".stripMargin

    def catalog3 =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"}
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

    val readDF3: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog3)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val re = readDF.join(readDF3,'cOrderSn ==='orderSn)
      .drop('orderSn).drop('cOrderSn).drop(readDF3.col("id"))
    val spls = functions.udf(spl _)

    val temp = re.withColumn("memberId",spls('memberId).cast("long").cast("string"))
      .withColumn("cycle",weekofyear(to_timestamp('modified)).cast("string"))
      .where('orderStatus==="202")
      .groupBy('memberId,'cycle).agg(collect_list('productName).cast("string").as("productName"),count('cycle).cast("string").as( "count"))
      .sort('cycle.asc)


    var re2 = temp.join(readDF2,temp.col("memberId")===readDF2.col("id"))
      .drop(temp.col("memberId")).drop(readDF2.col("id")).withColumn("id",monotonically_increasing_id)

    re2.show(false)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"cycle"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"cycle":{"cf":"cf", "col":"cycle", "type":"string"},
         |"count":{"cf":"cf", "col":"count", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"}
         |}
         |}""".stripMargin

    re2.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

  }
  def spl(str:String):String = {
    val len = str.length
    if(len > 3)
      return str.substring(len-3,len)
    else
      return str
  }
}
