
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, _}

//用户消费属性
object ConsumerModel {
  def main(args: Array[String]): Unit = {
    def catalog =
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

    //"codConfirmState":货到侍确认状态0无需未确认,1待确认,2确认通过可以发货,3确认无效,订单可以取消
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val readDF0: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val spls = functions.udf(spl _)

    val readDF = readDF0.withColumn("memberId",spls('memberId).cast("long").cast("string"))

    val result = readDF.withColumn("modified",to_timestamp('modified))
      .where('orderStatus==="202")
      .groupBy('memberId)
      .agg(min('modified) as "startTime", min('id) as "id" ,max('modified) as "endTime", count('memberId) as "orderCount",countDistinct(dayofyear('modified)) as "day" )
      .select('id,'memberId,'day,(datediff(('endTime),('startTime))+1).as("consumption"))
      .withColumn("Cycle",'consumption / 'day)
      .select('id,'Cycle,'memberId,when('Cycle < "7", "7日")
        .when('Cycle between (7,14) , "2周")
        .when('Cycle between (14,30) , "1月")
        .when('Cycle between (30,60) , "2月")
        .when('Cycle between (60,90) , "3月")
        .when('Cycle between (90,120) , "4月")
        .when('Cycle between (120,150) , "5月")
        .when('Cycle between (150,180) , "6月")
        .otherwise("六月以上")
        .as("周期")).drop("Cycle")

    val result2 =readDF
      .where('orderStatus==="202")
      .groupBy('memberId,'paymentName)
      .agg(count('paymentName) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .drop("count", "row_num")
      .select('memberId,'paymentName)


    val result3 = readDF
      .where('orderStatus==="202")
      .groupBy('memberId)
      .agg(avg('orderAmount) as "客单价")
      .select('客单价,'memberId,when('客单价 between(1,999),"1-999")
        .when('客单价 between(1000,2999),"1000-2999")
        .when('客单价 between(3000,4999),"3000-4999")
        .when('客单价 between(5000,9999),"5000-9999")
        .as("客户平均每单价格")
      ).drop("客单价")

    val result4 = readDF
      .where('orderStatus==="202")
      .groupBy('memberId)
      .agg(max('orderAmount.cast("long")) as "单最高")
      .select('单最高,'memberId,when('单最高 between(1,999),"1-999")
        .when('单最高 between(1000,2999),"1000-2999")
        .when('单最高 between(3000,4999),"3000-4999")
        .when('单最高 between(5000,9999),"5000-9999")
          .otherwise(">9999")
        .as('单笔最高)).drop("单最高")

    val result5 = readDF.withColumn("modified",to_timestamp('modified))
      .where('orderStatus==="202")
      .groupBy('memberId)
      .agg(min('modified) as "startTime", max('modified) as "endTime", count('memberId) as "orderCount"  )
      .select('memberId,'orderCount,(datediff(('endTime),('startTime))+1).as("consumption"))
      .withColumn("F",'orderCount/'consumption)
      .select('F,'memberId,when('F > "1.8", "高")
        .when('F between (1.6,1.8) , "中")
        .when('F < "1.6" , "低")
        .as("频率")).drop('F)
    //未付款确认可发货视为换货，未付款确认取消视为退货
    val result6=readDF
      .groupBy('memberId)
      .agg(count('memberId) as "orderCount",
        sum(when('orderStatus==="1" ,1).otherwise (0)) as "换货" ,
        sum(when('orderStatus==="0"  ,1).otherwise (0)) as "退货")
      .select('memberId,'orderCount,'换货,'退货)
      .withColumn("换货率",'换货/'orderCount)
      .withColumn("退货率",'退货/'orderCount)
      .select('换货率,'退货率,'memberId,when('换货率 > "0.01", "高")
        .when('换货率 between (0.005,0.01) , "中")
        .when('换货率 < "0.005" , "低")
        .as("换货的概率"),
        when('退货率 > "0.01", "高")
          .when('退货率 between (0.005,0.01) , "中")
          .when('退货率 < "0.005" , "低")
          .as("退货的概率")
      ).drop("换货率","退货率")

    val colRencency = "rencency"
    val colFrequency = "frequency"
    val colMoneyTotal = "moneyTotal"
    val colFeature = "feature"
    val colPredict = "predict"
    val days_range = 660

//    // 统计距离最近一次消费的时间
//    val recencyCol = datediff(date_sub(current_timestamp(), days_range), from_unixtime(max('finishTime))) as colRencency
//    // 统计订单总数
//    val frequencyCol = count('orderSn) as colFrequency
//    // 统计订单总金额
//    val moneyTotalCol = sum('orderAmount) as colMoneyTotal
//
//    val RFMResult = readDF.groupBy('memberId)
//      .agg(recencyCol, frequencyCol, moneyTotalCol)
//
//    //2.为RFM打分
//    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
//    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
//    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
//    val recencyScore: Column = functions.when((col(colRencency) >= 1) && (col(colRencency) <= 3), 5)
//      .when((col(colRencency) >= 4) && (col(colRencency) <= 6), 4)
//      .when((col(colRencency) >= 7) && (col(colRencency) <= 9), 3)
//      .when((col(colRencency) >= 10) && (col(colRencency) <= 15), 2)
//      .when(col(colRencency) >= 16, 1)
//      .as(colRencency)
//
//    val frequencyScore: Column = functions.when(col(colFrequency) >= 200, 5)
//      .when((col(colFrequency) >= 150) && (col(colFrequency) <= 199), 4)
//      .when((col(colFrequency) >= 100) && (col(colFrequency) <= 149), 3)
//      .when((col(colFrequency) >= 50) && (col(colFrequency) <= 99), 2)
//      .when((col(colFrequency) >= 1) && (col(colFrequency) <= 49), 1)
//      .as(colFrequency)
//
//    val moneyTotalScore: Column = functions.when(col(colMoneyTotal) >= 200000, 5)
//      .when(col(colMoneyTotal).between(100000, 199999), 4)
//      .when(col(colMoneyTotal).between(50000, 99999), 3)
//      .when(col(colMoneyTotal).between(10000, 49999), 2)
//      .when(col(colMoneyTotal) <= 9999, 1)
//      .as(colMoneyTotal)
//
//    val RFMScoreResult = RFMResult.select('memberId, recencyScore, frequencyScore, moneyTotalScore)
//
//    val vectorDF = new VectorAssembler()
//      .setInputCols(Array(colRencency, colFrequency, colMoneyTotal))
//      .setOutputCol(colFeature)
//      .transform(RFMScoreResult)
//
//    val kmeans = new KMeans()
//      .setK(7)
//      .setSeed(100)
//      .setMaxIter(2)
//      .setFeaturesCol(colFeature)
//      .setPredictionCol(colPredict)
//
//    // train model
//    val model = kmeans.fit(vectorDF)
//
//    val predicted = model.transform(vectorDF)
    //已通过RFMModel保存
def catalog2 =
  s"""{
     |"table":{"namespace":"default", "name":"consumer_rfm"},
     |"rowkey":"id",
     |"columns":{
     |"id":{"cf":"rowkey", "col":"id", "type":"long"},
     |"predict":{"cf":"cf", "col":"predict", "type":"string"}
     |}
     |}""".stripMargin
    val predicted: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val result0=predicted.select('memberId,'predict,when('predict==="0","超高")
      .when('predict==="1","高")
      .when('predict==="2","中上")
      .when('predict==="3","中")
      .when('predict==="4","中下")
      .when('predict==="5","低")
      .when('predict==="6","很低")as("用户价值")).drop("rencency","frequency","moneyTotal","feature","predict")
    //消费能力
    val result7: DataFrame=result.join(result2,result.col("memberId")===result2.col("memberId"))
      .drop(result2.col("memberId"))
    val result8: DataFrame=result7.join(result3,result7.col("memberId")===result3.col("memberId"))
      .drop(result3.col("memberId"))
    val result9: DataFrame=result8.join(result4,result8.col("memberId")===result4.col("memberId"))
      .drop(result4.col("memberId"))
    val result10: DataFrame=result9.join(result5,result9.col("memberId")===result5.col("memberId"))
      .drop(result5.col("memberId"))
    val result11: DataFrame=result10.join(result6,result10.col("memberId")===result6.col("memberId"))
      .drop(result6.col("memberId"))
    val result12: DataFrame=result11.join(result0,result11.col("memberId")===result0.col("memberId"))
      .drop(result0.col("memberId"))


    //points（积分）100：1,coupondCodeValue(优惠码)、giftCordAmounnt（活动卡，理解为券来分析有券必买）1：1
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"consumer"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |"周期":{"cf":"cf", "col":"周期", "type":"string"},
         |"paymentName":{"cf":"cf", "col":"paymentName", "type":"string"},
         |"客户平均每单价格":{"cf":"cf", "col":"客户平均每单价格", "type":"string"},
         |"单笔最高":{"cf":"cf", "col":"单笔最高", "type":"string"},
         |"频率":{"cf":"cf", "col":"频率", "type":"string"},
         |"换货的概率":{"cf":"cf", "col":"换货的概率", "type":"string"},
         |"退货的概率":{"cf":"cf", "col":"退货的概率", "type":"string"},
         |"用户价值":{"cf":"cf", "col":"用户价值", "type":"string"}
         |}
         |}""".stripMargin

    result12.write.mode("overwrite")
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