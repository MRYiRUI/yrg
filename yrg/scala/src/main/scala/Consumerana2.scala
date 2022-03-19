import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

//省钱能手(order/product=0.##折扣)
object Consumerana2 {
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
    val readDF:DataFrame =order_DF.withColumn("memberId",spls('memberId).cast("long").cast("string"))
    //省钱能手，评估：总订单数，折扣订单数，折扣力度

    val Model_1 =  readDF
      .where('orderStatus==="202")
      .groupBy('memberId)
      .agg(count('memberId) as 'buy_count,
        sum(when('orderAmount/'productAmount<1,1).otherwise(0))as("count_dis"),
        avg('orderAmount/'productAmount)as 'avgdiscount)
          .withColumn("colbuy",when('buy_count>80,5)
            .when('buy_count between(60,80),4)
            .when('buy_count between(40,60),3).
            when('buy_count between(20,40),2).
            when('buy_count <20,1))
          .withColumn("coldis",when('count_dis >15,5)
            .when('count_dis between(10,15),4)
            .when('count_dis between(6,10),3).
            when('count_dis between(3,6),2).
            when('count_dis <3,1))
          .withColumn("colavgdis",when('avgdiscount <0.95,5)
            .when('avgdiscount between(0.95,0.96),4)
            .when('avgdiscount between(0.96,0.97),3).
            when('avgdiscount between(0.97,0.98),2).
            when('avgdiscount >0.98,1))


    //CP模型
    val RFMScoreResult = Model_1.select('memberId,'colbuy, 'coldis,'colavgdis)
    val colFeature = "feature"
//

    val colPredict = "predict"
    val days_range = 660

    val vectorDF = new VectorAssembler()
      .setInputCols(Array("colbuy","coldis","colavgdis"))
      .setOutputCol(colFeature)
      .transform(RFMScoreResult)

    val kmeans = new KMeans()
      .setK(3)
      .setSeed(100)
      .setMaxIter(2)
      .setFeaturesCol(colFeature)
      .setPredictionCol(colPredict)
//
    // train model,021
    val model = kmeans.fit(vectorDF)
           model.clusterCenters.foreach(
          center => {
            println("Clustering Center:"+center)
          })
//    val predicted = model.transform(vectorDF)
//    val result = predicted.withColumnRenamed("memberId", "id")
//      .select('id,'predict,when('predict==="0","0")
//        .when('predict==="1","1").when('predict==="2","2")
//        .as("省钱能手")).drop("colbuy","coldis","colavgdis","feature","predict")
//    def catalogWrite =
//      s"""{
//         |"table":{"namespace":"default", "name":"consumer_anas"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
//         |"省钱能手":{"cf":"cf", "col":"省钱能手", "type":"string"}
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