package com.cpc.spark.small.tool

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/6/2.
  */

object InsertAdvUser {
  val mariadbUrl = "jdbc:mysql://10.9.180.16:3306/adv"
  val mariadbUrlNew = "jdbc:mysql://10.9.175.120/adv"

  val mariadbProp = new Properties()


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    println("cpc small tool InsertAdvUser running ... ")

    mariadbProp.put("user", "rd")
    mariadbProp.put("password", "rdv587@123")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")

    val ctx = SparkSession.builder()
      .appName("cpc small tool InsertAdvUser")
      .enableHiveSupport()
      .getOrCreate()

    //    val schema = StructType(Array(
    //      StructField("id", IntegerType, false),
    //      StructField("display_name", StringType, false),
    //      StructField("company", StringType, false),
    //      StructField("account_type", IntegerType, false)
    //    ))
    val where = Array[String]("acc_type in (0,1)", "sale_admin=0")
    var userAll = ctx.read.jdbc(mariadbUrlNew, "(SELECT CAST(id as SIGNED) id,display_name,company,CAST(belong as SIGNED) belong,CAST(account_type as SIGNED) account_type FROM user WHERE acc_type in (0,1) AND sale_admin=0) as xuser", mariadbProp)
    //userAll.write.mode(SaveMode.Overwrite).saveAsTable("dl_cpc.adv_user")

    var userAllNew = ctx.read.jdbc(mariadbUrl, "(SELECT CAST(id as SIGNED) id,display_name,company,CAST(belong as SIGNED) belong,CAST(account_type as SIGNED) account_type FROM user WHERE acc_type in (0,1) AND sale_admin=0) as xuser", mariadbProp)
    userAllNew
      .union(userAll)
      //userAllNew
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("dl_cpc.adv_user")
    ctx.stop()
  }
}
