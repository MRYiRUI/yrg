import LogModel.spl
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

//商品偏好
object PrefProductModel {
  def main(args: Array[String]): Unit = {
    def catalog_order =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"paymentStatus":{"cf":"cf", "col":"paymentStatus", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"paymentName":{"cf":"cf", "col":"paymentName", "type":"string"},
         |"orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |"finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_goods =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |"productType":{"cf":"cf", "col":"productType", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"}
         |}
         |}""".stripMargin
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val spls = functions.udf(spl _)
    val order_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_order)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val goods_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val readDF:DataFrame = goods_DF.join(order_DF,order_DF.col("orderSn")===goods_DF.col("cOrderSn"))
      .withColumn("memberId",spls('memberId).cast("long").cast("string"))
    val result = readDF
      .where('orderStatus==="202")
      .select('memberId,'productType).groupBy('memberId,'productType)
      .agg(count('productType)as("count"))
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .drop("count", "row_num")
        val result1 = readDF
          .where('orderStatus==="202")
          .select('memberId,'productName).groupBy('memberId,'productName)
          .agg(count('productName)as("count"))
          .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
          .where('row_num === 1)
          .drop("count", "row_num").withColumnRenamed("productName","productName1")
    val result2 = readDF
      .where('orderStatus==="202")
      .select('memberId,'productName).groupBy('memberId,'productName)
      .agg(count('productName)as("count"))
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 2)
      .drop("count", "row_num").withColumnRenamed("productName","productName2")
    val result3 = readDF
      .where('orderStatus==="202")
      .select('memberId,'productName).groupBy('memberId,'productName)
      .agg(count('productName)as("count"))
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 3)
      .drop("count", "row_num").withColumnRenamed("productName","productName3")
    val result4:DataFrame = result.join(result1,result.col("memberId")===result1.col("memberId")).drop(result1.col("memberId"))
    val result5:DataFrame = result4.join(result2,result4.col("memberId")===result2.col("memberId")).drop(result2.col("memberId"))
    val result6:DataFrame = result5.join(result3,result5.col("memberId")===result3.col("memberId")).drop(result3.col("memberId"))
      .withColumn("id", monotonically_increasing_id.cast("string"))

    result6.show(false)


            def catalogWrite =
          s"""{
             |"table":{"namespace":"default", "name":"pre_product"},
             |"rowkey":"id",
             |"columns":{
             |"id":{"cf":"rowkey", "col":"id", "type":"long"},
             |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
             |"productName1":{"cf":"cf", "col":"productName1", "type":"string"},
             |"productName2":{"cf":"cf", "col":"productName2", "type":"string"},
             |"productName3":{"cf":"cf", "col":"productName3", "type":"string"},
             |"productType":{"cf":"cf", "col":"productType", "type":"string"}
             |}
             |}""".stripMargin
        result6.write
          .option(HBaseTableCatalog.tableCatalog, catalogWrite)
          .option(HBaseTableCatalog.newTable, "5")
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()
  }
}