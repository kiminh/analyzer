package com.cpc.spark.hoursnapshot

import org.apache.spark.sql.{SaveMode, SparkSession}

object UnionIncomeHourlySnapshot {
  def main(args: Array[String]): Unit = {
    //参数小于1个
    if (args.length < 1) {
      System.err.println(
        s"""
           |usage: UnionIncomeHourlySnapshot table date hour
         """.stripMargin
      )
      System.exit(1)
    }

    //hive表名
    val hiveTable = args(0)
    val mysqlTable = args(1)
    val datee = args(2)
    val hour = args(3)

    //获得SparkSession
    val spark = SparkSession
      .builder()
      .appName("get income snapshot date = %s".format(datee))
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //定义url, user, psssword, driver, table
    val url = "jdbc:mysql://rr-2ze8n4bxmg3snxf7e.mysql.rds.aliyuncs.com:3306/union?useUnicode=true&characterEncoding=utf-8"
    val user = "rd"
    val passwd = "rdv587@123"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select * from %s where date='%s') as tmp".format(mysqlTable, datee)

    //从mysql获得最新charge
    val mysqlCharge = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    println("mysql schema" + mysqlCharge.printSchema())
    mysqlCharge.take(1).foreach(x => println("##### mysqlCharge:" + x))

    //从hive中获得当日charge数据
    val hiveCharge = spark.sql(
      s"""
         |select *
         |from dl_cpc.$hiveTable
         |where thedate='$datee'
      """.stripMargin)

    println("hive schema" + hiveCharge.printSchema())
    hiveCharge.take(1).foreach(x => println("##### hiveCharge:" + x))

    /**
      * 如果hive没数据，mysql数据直接写入hive，否则计算增量在写入hive
      */
    if (hiveCharge.count() == 0) {
      println("##############")
      if (mysqlCharge.take(1).length > 0) {
        mysqlCharge
          .write
          .mode(SaveMode.Overwrite)
          .parquet("/warehouse/dl_cpc.db/%s/thedate=%s/thehour=%s".format(hiveTable, datee, hour))
        println("###### mysqlCharge write hive successfully")
      } else {
        println("###### mysqlCharge为空")
      }

    } else {
      println("~~~~~~~~~~~~~~~~~~")

      //分组累加当日每小时的请求数，填充数，广告激励数，展示数，点击数，请求费用数，消费现金，消费优惠券
      val hiveCharge2 = hiveCharge.groupBy("media_id", "channel_id", "adslot_id", "date", "data_type")
        .sum("request", "served_request", "impression", "click", "impression2", "click2", "imp_media_income", "imp_channel_income",
          "click_media_income", "click_channel_income", "media_income", "channel_income", "media_income2", "rate", "click2_media_income")
        .toDF("media_id", "channel_id", "adslot_id", "adslot_type", "idea_id", "unit_id",
          "plan_id", "user_id", "date", "sum_request", "sum_served_request", "sum_activation",
          "sum_impression", "sum_click", "sum_fee", "sum_cash_cost", "sum_coupon_cost")

      println("hive2 schema" + hiveCharge2.printSchema())

      /**
        * 进行left outer join
        * 计算增量
        */

      val joinCharge = mysqlCharge
        .join(hiveCharge2, Seq("media_id", "channel_id", "adslot_id", "adslot_type",
          "idea_id", "unit_id", "plan_id", "user_id", "date"), "left_outer")
        .na.fill(0, Seq("sum_request", "sum_served_request", "sum_activation", "sum_impression",
        "sum_click", "sum_fee", "sum_cash_cost", "sum_coupon_cost")) //用0填充null

      val joinCharge2 = joinCharge
        .selectExpr(
          "media_id",
          "channel_id",
          "adslot_id",
          "adslot_type",
          "idea_id",
          "unit_id",
          "plan_id",
          "user_id",
          "date",
          "request - sum_request",
          "served_request - sum_served_request",
          "activation - sum_activation",
          "impression - sum_impression",
          "click - sum_click",
          "fee - sum_fee",
          "cash_cost - sum_cash_cost",
          "coupon_cost - sum_coupon_cost",
          "create_time",
          "modifid_time"
        )
        .toDF("media_id", "channel_id", "adslot_id", "adslot_type", "idea_id",
          "unit_id", "plan_id", "user_id", "date", "request", "served_request", "activation",
          "impression", "click", "fee", "cash_cost", "coupon_cost", "create_time", "modifid_time")


      joinCharge.take(1).foreach(x => println("##### joinCharge:" + x))


    }
  }
}
