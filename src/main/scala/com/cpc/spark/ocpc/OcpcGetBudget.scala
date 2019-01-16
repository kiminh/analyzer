package com.cpc.spark.ocpc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcGetBudget {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    val result = getBudget(date, hour, spark)
    result.show(10)
    result
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_ideaid_budget")
  }

  def getBudget(date: String, hour: String, spark: SparkSession) :DataFrame ={
    val url = "jdbc:mysql://rr-2ze8n4bxmg3snxf7e.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "rd"
    val passwd = "rdv587@123"
    val driver = "com.mysql.jdbc.Driver"
    val table =
      s"""
         |(SELECT
         |    p.id AS planid,
         |    idea.id as ideaid,
         |    LEAST((us.balance+us.coupon),p.budget-IFNULL(b.fee+b.coupon,0)) least_xbalance,
         |    IFNULL(b.fee+b.coupon,0) bcost
         |FROM plan p
         |LEFT JOIN
         |    bill b
         |ON p.id = b.plan_id AND b.date = CURDATE()
         |INNER JOIN
         |    unit u
         |ON u.plan_id = p.id
         |INNER JOIN
         |    user us
         |ON us.id = p.user_id
         |LEFT JOIN
         |    idea
         |ON
         |    idea.plan_id=p.id
         |WHERE
         |    ((b.fee+b.coupon) < p.budget OR b.plan_id is NULL)
         |AND
         |    (p.start_date <= CURDATE() OR p.start_date IS NULL)
         |AND
         |    (p.end_date >= CURDATE() OR p.end_date IS NULL)
         |AND
         |    u.status=0
         |AND
         |    p.status=0
         |AND
         |    us.status=2
         |AND
         |    u.ideas != ''
         |AND
         |    us.balance+us.coupon > 0
         |ORDER BY p.id DESC) as tmp
       """.stripMargin

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .select("planid", "ideaid", "least_xbalance", "bcost", "date", "hour")

    base

  }
}