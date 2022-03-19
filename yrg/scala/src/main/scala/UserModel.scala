
import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//用户基本表
object UserModel {
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

    //函数
    val fplace = functions.udf(place _)
    val fplaces = functions.udf(places _)
    //读取登陆表
    val log_DF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_log)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val logDF = log_DF.select('global_user_id,'user_agent,'loc_url,'log_time,
      when('loc_url like("%login%"),"登录页")
        .when('loc_url like("%order%"),"我的订单页")
        .when('loc_url like("%product%"),"商品页")
        .when('loc_url like("%item%"),"分类页")
        .when('loc_url like("%index%"),"首页")
        .otherwise("其他页面")
        .as("浏览页面")).groupBy('global_user_id).agg(max('log_time) as "log_time",
                sum(when('浏览页面==="登录页",1).otherwise(0))as("count_log"))
    //合并用户与登录
    val readDF:DataFrame = readDF0.join(logDF,readDF0.col("id")===logDF.col("global_user_id"))
        .drop(logDF.col("global_user_id"))
    val result = readDF.withColumn("birthday",to_timestamp('birthday))
      .withColumn("store",fplaces('mobile))
      .withColumn("province",fplace('mobile))
          .withColumn("currentTime",current_timestamp())
          .withColumn("lastlog",datediff('currentTime,'log_time))
          .withColumn("账户年龄",(datediff(from_unixtime('lastLoginTime),from_unixtime('registerTime))+1))
          .select('id,'birthday.cast("string"),'username,'email,'mobile,'province,
            'lastlog,'count_log,'账户年龄, 'store,
          when('gender === "1", "男")
            .when('gender === "2", "女")
            .otherwise("未知")
            .as("gender"),
          when('job === "1", "学生")
            .when('job === "2", "公务员")
            .when('job === "3", "军人")
            .when('job === "4", "警察")
            .when('job === "5", "教师")
            .when('job === "6", "白领")
            .otherwise("其他")
            .as("job"),
          when('politicalFace === "1","群众")
            .when('politicalFace === "2","党员")
            .when('politicalFace === "3","无党派人士")
            .otherwise("无")
            .as("politicalFace"),
          when('nationality === "1", "中国大陆")
            .when('nationality === "2", "中国香港")
            .when('nationality === "3", "中国澳门")
            .when('nationality === "4", "中国台湾")
            .when('nationality === "5", "其他")
            .otherwise("其他")
            .as("nationality"),
          when('marriage === "1", "未婚")
            .when('marriage === "2", "已婚")
            .when('marriage === "3", "离异")
            .otherwise("未知")
            .as("marriage"),
          when(year('birthday) between (1950,1959) , "50后")
            .when(year('birthday) between (1960,1969), "60后")
            .when(year('birthday) between (1970,1979), "70后")
            .when(year('birthday) between (1980,1989), "80后")
            .when(year('birthday) between (1990,1999), "90后")
            .when(year('birthday) between (2000,2009), "00后")
            .when(year('birthday) between (2010,2019), "10后")
            .when(year('birthday) between (2020,2029), "20后")
            .otherwise("其他")
            .as("age"),
          when(month('birthday) === "03" and  (dayofmonth('birthday) between(21,31)) , "白羊座")
            .when(month('birthday) === "03" and  (dayofmonth('birthday) between(1,20)) , "双鱼座")
            .when(month('birthday) === "04" and  (dayofmonth('birthday) between(1,20)) , "白羊座")
            .when(month('birthday) === "04" and  (dayofmonth('birthday) between(21,30)) , "金牛座")
            .when(month('birthday) === "05" and  (dayofmonth('birthday) between(1,21)) , "金牛座")
            .when(month('birthday) === "05" and  (dayofmonth('birthday) between(22,31)) , "双子座")
            .when(month('birthday) === "06" and  (dayofmonth('birthday) between(1,21)) , "双子座")
            .when(month('birthday) === "06" and  (dayofmonth('birthday) between(22,30)) , "巨蟹座")
            .when(month('birthday) === "07" and  (dayofmonth('birthday) between(1,22)) , "巨蟹座")
            .when(month('birthday) === "07" and  (dayofmonth('birthday) between(23,31)) , "狮子座")
            .when(month('birthday) === "08" and  (dayofmonth('birthday) between(1,22)) , "狮子座")
            .when(month('birthday) === "08" and  (dayofmonth('birthday) between(23,31)) , "处女座")
            .when(month('birthday) === "09" and  (dayofmonth('birthday) between(1,23)) , "处女座")
            .when(month('birthday) === "09" and  (dayofmonth('birthday) between(24,30)) , "天秤座")
            .when(month('birthday) === "10" and  (dayofmonth('birthday) between(1,23)) , "天秤座")
            .when(month('birthday) === "10" and  (dayofmonth('birthday) between(24,31)) , "天蝎座")
            .when(month('birthday) === "11" and  (dayofmonth('birthday) between(1,22)) , "天蝎座")
            .when(month('birthday) === "11" and  (dayofmonth('birthday) between(23,30)) , "射手座")
            .when(month('birthday) === "12" and  (dayofmonth('birthday) between(1,21)) , "射手座")
            .when(month('birthday) === "12" and  (dayofmonth('birthday) between(22,31)) , "摩羯座")
            .when(month('birthday) === "1" and  (dayofmonth('birthday) between(1,20)) , "摩羯座")
            .when(month('birthday) === "1" and  (dayofmonth('birthday) between(20,31)) , "水瓶座")
            .when(month('birthday) === "2" and  (dayofmonth('birthday) between(1,19)) , "水瓶座")
            .when(month('birthday) === "2" and  (dayofmonth('birthday) between(20,29)) , "双鱼座")
            .otherwise("其他")
            .as("Constellation"),
            when('lastlog <= 1, "1天以内")
              .when('lastlog between(1,7) , "7天以内")
              .when('lastlog between(7,14), "14天以内")
              .when('lastlog between(14,30), "30天以内")
              .otherwise("30天未登录")
              .as("log_recent"),
            when('count_log/'账户年龄 === 0, "无")
              .when('count_log/'账户年龄 between(0,5) , "较少")
              .when('count_log/'账户年龄 between(5,10), "一般")
              .when('count_log/'账户年龄 >10, "经常")
              .as("log_f"),
          when('is_blackList === "0", "非黑名单")
            .when('is_blackList === "1", "黑名单")
            .otherwise("其他")
            .as("is_blackList")
        ).drop("lastlog","count_log","账户年龄","registerTime","lastLoginTime")

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"username":{"cf":"cf", "col":"username", "type":"string"},
         |"gender":{"cf":"cf", "col":"gender", "type":"string"},
         |"email":{"cf":"cf", "col":"email", "type":"string"},
         |"mobile":{"cf":"cf", "col":"mobile", "type":"string"},
         |"province":{"cf":"cf", "col":"province", "type":"string"},
         |"store":{"cf":"cf", "col":"store", "type":"string"},
         |"birthday":{"cf":"cf", "col":"birthday", "type":"string"},
         |"log_recent":{"cf":"cf", "col":"log_recent", "type":"string"},
         |"log_f":{"cf":"cf", "col":"log_f", "type":"string"},
         |"job":{"cf":"cf", "col":"job", "type":"string"},
         |"politicalFace":{"cf":"cf", "col":"politicalFace", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"},
         |"marriage":{"cf":"cf", "col":"marriage", "type":"string"},
         |"age":{"cf":"cf", "col":"age", "type":"string"},
         |"Constellation":{"cf":"cf", "col":"Constellation", "type":"string"},
         |"is_blackList":{"cf":"cf", "col":"is_blackList", "type":"string"}
         |}
         |}""".stripMargin
    result.write.mode(SaveMode.Append)
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def place(moblie:String):String ={
    val url ="https://cx.shouji.360.cn/phonearea.php?number="+moblie;
    val httpClient = HttpClients.createDefault()
    val get = new HttpGet(url)
    val response = httpClient.execute(get)
    val result = EntityUtils.toString(response.getEntity(), "UTF-8")
    val jsonobj = JSON.parseObject(result)
    val a = jsonobj.get("data").toString
    val jsonobj2 = JSON.parseObject(a)
    val b = jsonobj2.get("province")
    return  b.toString;
  }
  def places(moblie:String):String ={
    val url ="https://cx.shouji.360.cn/phonearea.php?number="+moblie;
    val httpClient = HttpClients.createDefault()
    val get = new HttpGet(url)
    val response = httpClient.execute(get)
    val result = EntityUtils.toString(response.getEntity(), "UTF-8")
    val jsonobj = JSON.parseObject(result)
    val a = jsonobj.get("data").toString
    val jsonobj2 = JSON.parseObject(a)
    val b = jsonobj2.get("city")
    return  b.toString;
  }
  def spl(str:String):String = {
    val len = str.length
    if(len > 3)
      return str.substring(len-3,len)
    else
      return str
  }
}