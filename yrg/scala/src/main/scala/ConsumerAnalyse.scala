import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

//有券必买（giftCardAmount！=0与orderStatus的关联度）、省钱能手(order/product=0.##折扣)
object ConsumerAnalyse {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"couponCodeValue":{"cf":"cf", "col":"couponCodeValue", "type":"string"},
         |"orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"},
         |"giftCardAmount":{"cf":"cf", "col":"giftCardAmount", "type":"string"},
         |"productAmount":{"cf":"cf", "col":"productAmount", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_goods =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |"couponAmount":{"cf":"cf", "col":"couponAmount", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"},
         |"activityPrice":{"cf":"cf", "col":"activityPrice", "type":"string"},
         |"points":{"cf":"cf", "col":"points", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val goods_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val order_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val spls = functions.udf(spl _)
    val readDF:DataFrame =order_DF.join(goods_DF,order_DF.col("orderSn")===goods_DF.col("cOrderSn"), joinType = "left")
      .withColumn("memberId",spls('memberId).cast("long").cast("string"))
    //有券必买,礼品卡+优惠码，商品里有优惠劵

    //    val col_count = sum(when(('orderStatus==="202"),1).otherwise(0))as buy_count
    //    val colpay = sum(when(('orderStatus==="202"),'orderAmount).otherwise(0))as pay_total
    val Model_1 =  readDF.groupBy('memberId)
      .agg(sum(when(('orderStatus==="202" and('couponAmount =!=0)),1).otherwise(0))as 'buy_coupon,
        sum(when('orderStatus==="202"and ('giftCardAmount =!=0),1).otherwise(0))as 'buy_card
        ,sum(when(('orderStatus==="202" and 'activityPrice =!= 0),1).otherwise(0))as 'buy_activity
      )
      .withColumn("buy_coupon",when('buy_coupon>5,5)
        .when('buy_coupon ===5,4)
        .when('buy_coupon ===4,3).
        when('buy_coupon ===3,2).
        when('buy_coupon <3,1))
      .withColumn("buy_card",when('buy_card >4,5)
        .when('buy_card ===4,4)
        .when('buy_card ===3,3).
        when('buy_card ===2,2).
        when('buy_card <2,1))
      .withColumn("buy_activity",when('buy_activity >5,5)
        .when('buy_activity ===5,4)
        .when('buy_activity ===4,3).
        when('buy_activity between(2,4),2).
        when('buy_activity <2,1))

    //CP模型
    val RFMScoreResult = Model_1.select('memberId,'buy_coupon, 'buy_card,'buy_activity)
    val colFeature = "feature"

    // 统计订单总数
    val colPredict = "predict"
    val days_range = 660
    // 统计订单总金额
    val vectorDF = new VectorAssembler()
      .setInputCols(Array("buy_coupon","buy_card","buy_activity"))
      .setOutputCol(colFeature)
      .transform(RFMScoreResult)

    val kmeans = new KMeans()
      .setK(3)
      .setSeed(100)
      .setMaxIter(2)
      .setFeaturesCol(colFeature)
      .setPredictionCol(colPredict)

    // train model，2>0>1,coupon,activity,card
    val model = kmeans.fit(vectorDF)
           model.clusterCenters.foreach(
          center => {
            println("Clustering Center:"+center)
          })
//    val predicted = model.transform(vectorDF)
//
//    val result = predicted.withColumnRenamed("memberId", "id")
//      .select('id,'predict,when('predict==="0","0")
//        .when('predict==="1","1").when('predict==="2","2")
//        .as("有券必买")).drop("colcount","buy_coupon","buy_card","buy_activity","feature","predict")
//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"consumer_ana1"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
//         |"有券必买":{"cf":"cf", "col":"有券必买", "type":"string"}
//         |}
//         |}""".stripMargin
//    result.write.mode("overwrite")
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
  }
  def spl(str:String):String = {
    val len = str.length
    if(len > 3)
      return str.substring(len-3,len)
    else
      return str
  }
}