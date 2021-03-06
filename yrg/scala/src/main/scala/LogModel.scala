
import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//对登录的分析
object LogModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
         |"modified":{"cf":"cf", "col":"modified", "type":"string"},
         |"registerTime":{"cf":"cf", "col":"registerTime", "type":"string"},
         |"lastLoginTime":{"cf":"cf", "col":"lastLoginTime", "type":"string"},
         |"is_blackList":{"cf":"cf", "col":"is_blackList", "type":"string"}
         |}
         |}""".stripMargin
    def catalog_log =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"user_agent":{"cf":"cf", "col":"user_agent", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"}
         |}
         |}""".stripMargin
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
    val readDF0: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val order_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_order)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    val goods_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    val good_t = goods_DF.select('cOrderSn,'productName)
//    val order_t= order_DF.select('orderSn,'memberId,'orderStatus)
    val good_order:DataFrame = goods_DF.join(order_DF,order_DF.col("orderSn")===goods_DF.col("cOrderSn"))
      .withColumn("memberId",spls('memberId).cast("long").cast("string"))
    //good_order.orderBy('memberId).show()
//    //浏览的商品
//    val a = good_order.select('memberId,'productName).groupBy("memberId")
//      .agg(collect_set('productName).as("product_scan").cast("string")as("product_scan"))
//    //用户购买商品与最多的商品种类
//    val b = good_order.select('memberId,'productName,'orderStatus,'productType)
//      .where('orderStatus==="202")
//      .groupBy("memberId","productType")
//      .agg(collect_set('productName).as("product_buy").cast("string")as("product_buy"),count("productType")as "count")
//      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
//      .where('row_num === 1)
//      .drop("count", "row_num")
//    //用户购买最多的商品
//    val c = good_order.select('memberId,'productName,'orderStatus)
//      .where('orderStatus==="202")
//      .groupBy("memberId","productName")
//      .agg(count("productName") as "count")
//      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
//      .where('row_num === 1)
//      .drop("count", "row_num")
//    //
//    //偏好的品牌，所给品牌皆为海尔旗下
//    val d = good_order.select('memberId,'productName,'orderStatus,
//      when('productName like "%卡萨帝%","卡萨帝")
//        .when('productName like "%MOOKA%","摩卡")
//        .when('productName like "%KFR%","小超人")
//        .when('productName like "%统帅%","统帅")
//        .when('productName like "%海尔%","海尔")
//        .otherwise("其他")
//        .as("BrandPreference"))
//      .where('orderStatus==="202")
//      .groupBy("memberId","BrandPreference")
//      .agg(sum(when('BrandPreference==="其他",1).when('BrandPreference==="海尔",1).otherwise(10)) as "count")
//      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
//      .where('row_num === 1)
//      .drop("count", "row_num").withColumnRenamed("memberId","id")
//    //用户浏览与购买的商品连接，用户三偏好的连接
//    val result1: DataFrame=a.join(b,a.col("memberId")===b.col("memberId"))
//      .drop(b.col("memberId"))
//    val result2: DataFrame=result1.join(c,result1.col("memberId")===c.col("memberId"))
//      .drop(c.col("memberId"))
//    val result3: DataFrame=result2.join(d,result2.col("memberId")===d.col("id"))
//
//
//    //读取登陆表
//    val log_DF: DataFrame = spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalog_log)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//    //浏览记录分析，目前暂无完全合并的需求
//    val logDF_pre = log_DF.drop('id)
//      .select('global_user_id,'user_agent,'loc_url,'log_time,
//        when('loc_url like("%login%"),"登录页")
//          .when('loc_url like("%order%"),"我的订单页")
//          .when('loc_url like("%product%"),"商品页")
//          .when('loc_url like("%item%"),"分类页")
//          .when('loc_url like("%index%"),"首页")
//          .otherwise("其他页面")
//          .as("浏览页面"),
//        when(hour('log_time) between(0,7),"0点-7点")
//          .when(hour('log_time) between(8,12),"8点-12点")
//          .when(hour('log_time) between(13,17),"13点-17点")
//          .when(hour('log_time) between(18,21),"18点-21点")
//          .when(hour('log_time) between(22,24),"22点-24点")
//          .as("浏览时段"),
//        when('user_agent like("%Android%"),"Android")
//          .when('user_agent like("%iPhone%"),"IOS")
//          .when('user_agent like("%WOW%"),"Window")
//          .when('user_agent like("%Linux%"),"Linux")
//          .otherwise("Mac")
//          .as("设备类型")
//
//      )
//      .withColumn("pre_time", lag('log_time,1) over Window.partitionBy('global_user_id).orderBy('log_time.desc))
//      .withColumn("浏览时间",when(isnull('pre_time),"0").when((unix_timestamp('pre_time)-unix_timestamp('log_time))<60,"1分钟内")
//        .when((unix_timestamp('pre_time)-unix_timestamp('log_time)) between(60,300),"1-5分钟")
//        .when((unix_timestamp('pre_time)-unix_timestamp('log_time))>300,"5分钟以上"))
//      val log_tbl1:DataFrame = logDF_pre.groupBy('global_user_id).agg(collect_list('浏览页面).as("浏览页面").cast("string")as("浏览页面")
//        ,collect_list('浏览时段).as("浏览时段").cast("string")as("浏览时段")
//      ,collect_list('设备类型).as("设备类型").cast("string") as("设备类型")
//        ,collect_list('浏览时间).as("浏览时间").cast("string")as("浏览时间"))
//    .withColumnRenamed("global_user_id","id")
//    //访问不同种类页面频率，只有一次log_time的记为偶尔
//    val log_scan = logDF_pre.groupBy('global_user_id,'浏览页面).agg(datediff(max('log_time),min('log_time)) as "浏览总时间",count('浏览页面) as ("count_scan"))
//      .withColumn("访问频率",when('count_scan/'浏览总时间>1.5,"经常").
//        when('count_scan/'浏览总时间 between(1,1.5),"很少").
//        when('count_scan/'浏览总时间 between(0,1.5),"偶尔")
//        .when('count_scan/'浏览总时间===0,"从不").otherwise("偶尔"))
//    .groupBy('global_user_id).agg(collect_list('浏览页面).as("访问页面类别").cast("string")as("访问页面类别")
//      ,collect_list('访问频率).as("访问频率").cast("string")as("访问频率")).drop("count_scan")
//连接
//val log_tbl:DataFrame = log_tbl1.join(log_scan,log_scan.col("global_user_id")===log_tbl1.col("id"))
//    log_tbl.show()
    //为避免数据混乱，不再做下面的连接,log_tbl为浏览记录，result3为偏好记录+浏览与购买的商品
    //连接log记录与分析结果,
//    val result4: DataFrame=result3.join(log_tbl,result3.col("memberId")===log_tbl.col("global_user_id"))
//      .drop(log_tbl.col("global_user_id"))
//    def catalogWrite1 =
//      s"""{
//         |"table":{"namespace":"default", "name":"log_consumer"},
//         |"rowkey":"id",
//         |"columns":{
//         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
//         |"memberId":{"cf":"cf", "col":"memberId", "type":"long"},
//         |"product_scan":{"cf":"cf", "col":"product_scan", "type":"string"},
//         |"productType":{"cf":"cf", "col":"productType", "type":"string"},
//         |"product_buy":{"cf":"cf", "col":"product_buy", "type":"string"},
//         |"productName":{"cf":"cf", "col":"productName", "type":"string"},
//         |"BrandPreference":{"cf":"cf", "col":"BrandPreference", "type":"string"}
//         |}
//         |}""".stripMargin
//    result3.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite1)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
    //曾尝试连接所有表，结构不易分析，舍弃
    //    val user_alys:DataFrame =   result.join(result3,result.col("id")===result3.col("memberId"))
    //      .drop(result3.col("memberId"))
    //      .withColumnRenamed("浏览商品","scan_product")
    //      .withColumnRenamed("购买商品","buy_product")
    //      .withColumnRenamed("最近登录","recent_log")
    //      .withColumnRenamed("登陆频率","F_log")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"log_scan"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"long"},
         |"浏览页面":{"cf":"cf", "col":"浏览页面", "type":"string"},
         |"浏览时段":{"cf":"cf", "col":"浏览时段", "type":"string"},
         |"浏览时间":{"cf":"cf", "col":"浏览时间", "type":"string"},
         |"设备类型":{"cf":"cf", "col":"设备类型", "type":"string"},
         |"访问页面类别":{"cf":"cf", "col":"访问页面类别", "type":"string"},
         |"访问频率":{"cf":"cf", "col":"访问频率", "type":"string"}
         |}
         |}""".stripMargin

//    log_tbl.write
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
