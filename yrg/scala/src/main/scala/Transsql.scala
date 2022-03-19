
import java.util.Properties

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Transsql {
  //log_job
  def main(args: Array[String]): Unit = {
    def catalog =
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

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    readDF.createOrReplaceTempView("consumer_rfm")

    val prop =new Properties()

    val url ="jdbc:mysql://192.168.101.118:3306/spark?user=root&password=085334&useUnicode=true&characterEncoding=utf8"

    readDF.write.jdbc(url,"consumer_rfm",prop)

    spark.stop()
  }
}
