import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//不同年龄对品牌的喜好
object Brand_age {
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
    val order_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_order)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val goods_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val spls = functions.udf(spl _)
    val good_order:DataFrame = goods_DF.join(order_DF,order_DF.col("orderSn")===goods_DF.col("cOrderSn"))
      .withColumn("memberId",spls('memberId).cast("long").cast("string"))
        //偏好的品牌，所给品牌皆为海尔旗下
        val con_DF = good_order.select('memberId,'productName,'orderStatus,
          when('productName like "%卡萨帝%","卡萨帝")
            .when('productName like "%MOOKA%","摩卡")
            .when('productName like "%KFR%","小超人")
            .when('productName like "%统帅%","统帅")
            .when('productName like "%海尔%","海尔")
            .otherwise("无偏好")
            .as("BrandPreference"))
          .where('orderStatus==="202")
          .groupBy("memberId","BrandPreference")
          .agg(sum(when('BrandPreference==="无偏好",0).when('BrandPreference==="海尔",1).otherwise(10)) as "count")
          .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
          .where('row_num === 1)
          .drop("count", "row_num").withColumnRenamed("memberId","id")
    def catalog_a =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"age":{"cf":"cf", "col":"age", "type":"string"}
         |}
         |}""".stripMargin


    val user_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_a)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val o_u:DataFrame = user_DF.join(con_DF,user_DF.col("id")===con_DF.col("id"))
      .drop(con_DF.col("id"))
      .groupBy('age).agg(
      sum(when('BrandPreference==="海尔",1).otherwise(0)).cast("long").cast("string")as("海尔"),
      sum(when('BrandPreference==="卡萨帝",1).otherwise(0)).cast("long").cast("string")as("卡萨帝"),
      sum(when('BrandPreference==="摩卡",1).otherwise(0)).cast("long").cast("string")as("摩卡"),
      sum(when('BrandPreference==="小超人",1).otherwise(0)).cast("long").cast("string")as("小超人"),
      sum(when('BrandPreference==="统帅",1).otherwise(0)).cast("long").cast("string")as("统帅"),
      sum(when('BrandPreference==="无偏好",1).otherwise(0)).cast("long").cast("string")as("无偏好")
    ).withColumn("id", monotonically_increasing_id.cast("string"))
    o_u.show()
    //海尔、卡萨帝、摩卡、小超人、统帅
    def catalog_w =
      s"""{
         |"table":{"namespace":"default", "name":"Brand_age"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"age":{"cf":"cf", "col":"age", "type":"string"},
         |"海尔":{"cf":"cf", "col":"海尔", "type":"string"},
         |"卡萨帝":{"cf":"cf", "col":"卡萨帝", "type":"string"},
         |"摩卡":{"cf":"cf", "col":"摩卡", "type":"string"},
         |"小超人":{"cf":"cf", "col":"小超人", "type":"string"},
         |"统帅":{"cf":"cf", "col":"统帅", "type":"string"},
         |"无偏好":{"cf":"cf", "col":"无偏好", "type":"string"}
         |}
         |}""".stripMargin
    o_u.write
      .option(HBaseTableCatalog.tableCatalog, catalog_w)
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