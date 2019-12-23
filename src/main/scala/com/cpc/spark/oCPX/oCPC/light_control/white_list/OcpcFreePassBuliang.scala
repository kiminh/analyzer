package com.cpc.spark.oCPX.oCPC.light_control.white_list

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcFreePassBuliang {
  def main(args: Array[String]): Unit = {
    /*
    oCPC零门槛实验
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    println(s"parameters: date=$date, hour=$hour, version=$version")

    // 获取广告单元
    val unit = getUnitData(date, hour, spark).cache()
    unit.show(10)

    // oCPC补量实验
    val ocpcBuliang = ocpcBuliangUsers(spark)

    // 数据关联
    val joinData = unit
        .join(ocpcBuliang, Seq("userid"), "inner")

//    joinData
//        .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191216b")

    joinData
      .select("unitid", "userid", "conversion_goal", "ocpc_status")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_free_pass_buliang_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_free_pass_buliang_hourly")




  }

  def ocpcBuliangUsers(spark: SparkSession) = {
    val dataRaw = spark.read.textFile("/user/cpc/lixuejian/online/select_hidden_tax_user/ocpc_hidden_tax_user.list")
//    val dataRaw = spark.read.textFile("/user/cpc/wangjun/ocpc/test/ocpc_hidden_tax_user.list")

    val data = dataRaw
      .select("value")
      .withColumn("userid", udfGetItem(0, " ")(col("value")))
      .withColumn("is_open", udfGetItem(1, " ")(col("value")))
      .select("userid", "is_open")
      .filter(s"is_open = 1")
      .distinct()

    data
  }

  def udfGetItem(index: Int, splitter: String) = udf((value: String) => {
    val valueItem = value.split(splitter)(index)
    val result = valueItem.toInt
    result
  }
  )



  def getUnitData(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, cast(conversion_goal as char) as conversion_goal, target_medias, is_ocpc, ocpc_status, create_time from adv.unit where is_ocpc = 1 and ocpc_status in (0, 3)) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .selectExpr("unitid",  "userid", "cast(conversion_goal as int) conversion_goal", "cast(is_ocpc as int) is_ocpc", "ocpc_status", "target_medias", "create_time")
      .distinct()

    resultDF.show(10)
    resultDF
  }

}