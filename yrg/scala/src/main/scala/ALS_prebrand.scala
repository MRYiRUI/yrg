import LogModel.spl
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

object ALS_prebrand {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenderName")
      .master("local")
      .getOrCreate()

    import spark.implicits._
//
//    def catalog_order =
//      s"""{
//         |"table":{"namespace":"default", "name":"tbl_orders"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
//         |"paymentStatus":{"cf":"cf", "col":"paymentStatus", "type":"string"},
//         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
//         |"paymentName":{"cf":"cf", "col":"paymentName", "type":"string"},
//         |"orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"},
//         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
//         |"orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
//         |"finishTime":{"cf":"cf", "col":"finishTime", "type":"string"}
//         |}
//         |}""".stripMargin
//    def catalog_goods =
//      s"""{
//         |"table":{"namespace":"default", "name":"tbl_goods"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
//         |"productName":{"cf":"cf", "col":"productName", "type":"string"}
//         |}
//         |}""".stripMargin
//    def catalog_u =
//      s"""{
//         |"table":{"namespace":"default", "name":"user"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"username":{"cf":"cf", "col":"username", "type":"string"}
//         |}
//         |}""".stripMargin
//    val spls = functions.udf(spl _)
//    val order_DF: DataFrame = spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalog_order)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//    val goods_DF: DataFrame = spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//    val user_DF: DataFrame = spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalog_u)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//    val good_order:DataFrame = goods_DF.join(order_DF,order_DF.col("orderSn")===goods_DF.col("cOrderSn"))
//      .withColumn("memberId",spls('memberId).cast("long").cast("string")).drop(order_DF.col("id"))
//    val user_pretype:DataFrame = user_DF
//      .join(good_order,user_DF.col("id")===good_order.col("memberId"),"left")
//      .drop(good_order.col("memberId")).drop(good_order.col("id"))



//    val ratingDF = user_pretype.select(
//      'id.as("userId").cast(DataTypes.IntegerType),'productName,
//      when('productName like "%统帅%",1).
//        when('productName like "%卡萨帝%",2).
//        when('productName like "%摩卡%",3).
//        when('productName like "%小超人%",4).
//        when('productName like "%海尔%",5).as("brand").cast(DataTypes.IntegerType)
//    )
//      .filter('Brand.isNotNull)
//      .groupBy('userId, 'Brand)
//      .agg(count('Brand) as "rating")

//    val als = new ALS()
//      .setUserCol("userId")
//      .setItemCol("Brand")
//      .setRatingCol("rating")
//      .setPredictionCol("predict")
//      .setColdStartStrategy("drop")
//      .setAlpha(10)
//      .setMaxIter(10)
//      .setRank(10)
//      .setRegParam(1.0)
//      .setImplicitPrefs(true)
//
//        val model: ALSModel = als.fit(ratingDF)
//
//        model.save("model/product/als2")
//
    val model = ALSModel.load("model/product/als2")

    val predict2StringFunc = udf(predict2String _)

    // 为每个用户推荐
    val result: DataFrame = model.recommendForAllUsers(3)
      .withColumn("favorBrand", predict2StringFunc('recommendations))
      .withColumnRenamed("userId", "id")
      .drop('recommendations)
      .select('id.cast(LongType), 'favorBrand)

    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"ALS_favorBrand"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "favorBrand":{"cf":"cf", "col":"favorBrand", "type":"string"}
         |   }
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }

  def predict2String(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("Brand")).mkString(",")
  }
}