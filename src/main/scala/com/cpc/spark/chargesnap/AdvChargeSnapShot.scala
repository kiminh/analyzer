package com.cpc.spark.chargesnap

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}


object AdvChargeSnapShot {
  def main(args: Array[String]): Unit = {

    //参数小于1个
    if (args.length < 1) {
      System.err.println(
        s"""
           |usage: advchargesnapshot table date hour
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
      .appName("get charge snapshot date = %s".format(datee))
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //定义url, user, psssword, driver, table
    val url = "jdbc:mysql://rr-2ze8n4bxmg3snxf7e.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
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

    mysqlCharge.take(1).foreach(x => println("##### mysqlCharge:" + x))

    //从hive中获得当日charge数据
    val hiveCharge = spark.sql(
      s"""
         |select *
         |from dl_cpc.$hiveTable
         |where thedate='$datee'
      """.stripMargin)

    mysqlCharge.take(1).foreach(x => println("##### hiveCharge:" + x))

    /**
      * 如果hive没数据，mysql数据直接写入hive，否则计算增量在写入hive
      */
    if (hiveCharge.count() == 0) {
      println("##############")
      if (mysqlCharge.take(1).length > 0) {
        mysqlCharge.write
          .mode(SaveMode.Overwrite)
          .parquet("/warehouse/dl_cpc.db/%s/thedate=%s/thehour=%s".format(hiveTable, datee, hour))
        println("###### mysqlCharge write hive successfully")
      } else {
        println("###### mysqlCharge为空")
      }

    } else {
      println("~~~~~~~~~~~~~~~~~~")
      //分组累加当日每小时的请求数，填充数，广告激励数，展示数，点击数，请求费用数，消费现金，消费优惠券
      val hiveCharge2 = hiveCharge.groupBy("media_id", "channel_id", "adslot_id",
        "adslot_type", "idea_id", "unit_id", "plan_id", "user_id", "date")
        .sum("request", "served_request", "activation", "impression", "click",
          "fee", "cash_cost", "coupon_cost")
        .toDF("media_id", "channel_id", "adslot_id", "adslot_type", "idea_id", "unit_id",
          "plan_id", "user_id", "date", "sum_request", "sum_served_request", "sum_activation",
          "sum_impression", "sum_click", "sum_fee", "sum_cash_cost", "sum_coupon_cost")

      /**
        * 进行left outer join
        * 计算增量
        */

      val joinCharge = mysqlCharge
        .join(hiveCharge2, Seq("media_id", "channel_id", "adslot_id", "adslot_type",
          "idea_id", "unit_id", "plan_id", "user_id", "date"), "left_outer")
        .na.fill(0, Seq("sum_request", "sum_served_request", "sum_activation", "sum_impression",
        "sum_click", "sum_fee", "sum_cash_cost", "sum_coupon_cost")) //用0填充null
        .select(
        mysqlCharge("media_id"),
        mysqlCharge("channel_id"),
        mysqlCharge("adslot_id"),
        mysqlCharge("adslot_type"),
        mysqlCharge("idea_id"),
        mysqlCharge("unit_id"),
        mysqlCharge("plan_id"),
        mysqlCharge("user_id"),
        mysqlCharge("date"),
        mysqlCharge("request") - hiveCharge2("sum_request"),
        mysqlCharge("served_request") - hiveCharge2("sum_served_request"),
        mysqlCharge("activation") - hiveCharge2("sum_activation"),
        mysqlCharge("impression") - hiveCharge2("sum_impression"),
        mysqlCharge("click") - hiveCharge2("sum_click"),
        mysqlCharge("fee") - hiveCharge2("sum_fee"),
        mysqlCharge("cash_cost") - hiveCharge2("sum_cash_cost"),
        mysqlCharge("coupon_cost") - hiveCharge2("sum_coupon_cost"),
        mysqlCharge("create_time"),
        mysqlCharge("modifid_time")
      )
        .toDF("media_id", "channel_id", "adslot_id", "adslot_type", "idea_id",
          "unit_id", "plan_id", "user_id", "date", "request", "served_request", "activation",
          "impression", "click", "fee", "cash_cost", "coupon_cost", "create_time", "modifid_time")

      println("########" + joinCharge.printSchema())
      joinCharge.take(1).foreach(x => println("##### joinCharge:" + x))

      if (joinCharge.take(1).length > 0) {
        joinCharge.write
          .mode(SaveMode.Overwrite)
          .parquet("/warehouse/dl_cpc.db/%s/thedate=%s/thehour=%s".format(hiveTable, datee, hour))

        println("###### joinCharge write hive successfully")
      } else {
        println("###### joinCharge为空")
      }

    }

    spark.sql(
      """
        |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`thedate` = "%s", `thehour` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/%s/thedate=%s/thehour=%s'
      """.stripMargin.format(hiveTable, datee, hour, hiveTable, datee, hour))

    println("~~~~~~write charge to hive successfully")
  }


}
