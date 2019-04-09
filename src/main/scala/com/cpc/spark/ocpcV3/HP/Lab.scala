package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Date
import org.apache.spark.sql.functions._

object Lab {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Lab").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val date = args(0).toString
    val targetApp = args(1).toString
    val usedOrInstalled = args(2).toString

    assert(usedOrInstalled == "u" || usedOrInstalled == "i", "please input u for used or i for installed")
    println("date: " + date + "; targetApp: " + targetApp + "; usedOrInstalled: " + usedOrInstalled)
    val if_used = if (usedOrInstalled == "u") true else false

    val baseData = getBaseData(spark, date, targetApp, if_used).map(x => (x._1, x._2.distinct.mkString(","))).toDF("uid", "appNames")
    baseData.write.mode("overwrite").saveAsTable("test.associationRule_base_data_sjq")
  }

  def getBaseData(spark: SparkSession, date: String, targetApp: String, if_used: Boolean) = {
    import spark.implicits._
    val sqlRequest =
      s"""
         |select
         | uid,
         | concat_ws(',', used_pkgs) as pkgs_used,
         | concat_ws(',', app_name)  as pkgs_installed
         | from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin

    println(sqlRequest)

    val df = spark.sql(sqlRequest) //DataFrame: uid, pkgs_used, pkgs_installed

    val df0 = df.rdd.map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs_installed").split(",")))
      .flatMap(x => {
        val uid = x._1
        val pkgs = x._2
        val lb = scala.collection.mutable.ListBuffer[UidComb]()
        for (comb <- pkgs) {
          lb += UidComb(uid, comb)
        }
        lb.distinct
      }).toDF("uid", "comb").rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("comb")))
      .map(x => {
        val uid = x._1
        val arr = x._2.split("-", 2)
        if (arr.size == 2) (uid, arr(0), arr(1)) else (uid, arr(0), "")
      }).distinct.toDF("uid", "pkg", "appName").persist() //DataFrame: uid, pkg_installed, appName

    val df1 = df.rdd.map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs_used").split(","))).filter(_._2.size > 0)
      .flatMap(x => {
        val uid = x._1
        val pkgs_used = x._2
        val lb = scala.collection.mutable.ListBuffer[UidPkg]()
        for (pkg <- pkgs_used) {
          lb += UidPkg(uid, pkg, 1)
        }
        lb.distinct
      }).toDF("uid", "pkg", "tag") // DataFrame: uid, pkg_used

    val df2 = df0.join(df1, Seq("uid", "pkg"), "left")
      .select("uid", "appName", "tag")
      .withColumn("ifused", when(col("tag") === 1, lit(1)).otherwise(lit(0)))
      .select("uid", "appName", "ifused").distinct()

    val df21 = if (if_used) {
      df2.filter("ifused = 1").select("uid", "appName")
    }
    else {
      df2.select("uid", "appName")
    } //DataFrame: uid, appName

    val df3 = df21.groupBy("appName")
      .agg(
        countDistinct("uid").alias("count")
      ).select("appName", "count") // DataFrame: appName, count

    df3.write.mode("overwrite").saveAsTable("test.appCount_sjq")

    val countUpper = df21.agg(countDistinct("uid").alias("uidn")).rdd.map(x => x.getAs[Int]("uidn")).reduce(_ + _) * 0.2
    val countLower = df3.where(s"appName = '${targetApp}'").select("count").rdd.map(x => x.getAs[Int]("count")).reduce(_ + _) * 0.7
    val filter_condition =
      s"""    appName not like '%小米%'
         |and appName not like '%OPPO%'
         |and appName not like '%反馈%'
         |and appName not like '%设置%'
         |and appName not like '%相册%'
         |and appName not like '%联系人%'
         |and appName not like '%文件%'
         |and appName not like '%邮件%'
         |and appName not like '%时钟%'
         |and appName not like '%录音机%'
         |and appName not like '%扫一扫%'
         |and appName not in ('计算器', '指南针', '天气', '便签')
         |and count between ${countLower} and ${countUpper}""".stripMargin
    println(filter_condition)

    val df4 = df3.filter(filter_condition) //DataFrame: appName, count

    val A = df21.where(s"appName = '${targetApp}'").select("uid").distinct() //含targetApp的uid的集合
    val B = df21.join(A, Seq("uid")).select("appName").distinct() //A安装的appName的集合

    val df5 = df21.join(B, Seq("appName")).select("uid", "appName").join(df4, Seq("appName")).select("uid", "appName") //DataFrame: uid, appName

    val result = df5.rdd
      .map(x => (x.getAs[String]("uid"), Array(x.getAs[String]("appName"))))
      .reduceByKey((appArr1, appArr2) => appArr1 ++ appArr2)

    result
  }

  case class UidComb(var uid: String, var comb: String)

  case class UidPkg(var uid: String, var pkg: String, var tag: Int)

}








