package com.cpc.spark.oCPX.oCPC.pay

import com.cpc.spark.oCPX.OcpcTools.udfConcatStringInt
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

@deprecated
object OcpcCompensateBackup {
  def main(args: Array[String]): Unit = {
    /*
    按照七天周期计算赔付数据
    1. 计算当天所有单元的点击、消费、转化、平均cpagiven、平均cpareal、赔付金额
    2. 获取这批单元在赔付周期中的起始时间
    3. 如果当前为周期第一天，则重新落表，否则，叠加上前一天的历史数据
    4. 数据落表，需包括周期编号，是否周期第一天
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    // 计算当天数据
    val data = getOcpcCompensate(spark)

    data
      .withColumn("date", lit(date))
      .repartition(10)
//      .write.mode("overwrite").saveAsTable("test.ocpc_compensate_backup_daily20191130")
//      .write.mode("overwrite").insertInto("test.ocpc_compensate_backup_daily")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_compensate_backup_daily")


  }

  def getOcpcCompensate(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(SELECT unit_id as unitid, user_id as userid, compensate_key, ocpc_charge_time, cost, conversion, cpareal, cpagiven, pay, cpc_flag, logic_version, create_time FROM ocpc_compensate) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
        .selectExpr("unitid", "userid", "compensate_key", "cast(ocpc_charge_time as string) ocpc_charge_time", "cost", "conversion", "cpareal", "cpagiven", "pay", "cpc_flag", "logic_version", "cast(create_time as string) as create_time")

    resultDF.show(10)
    resultDF
  }



}
