package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Date

object Lab {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Lab").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val targetApp = args(1).toString
    println("date: " + date + "; targetApp: " + targetApp)
    import spark.implicits._

    val baseData = getBaseData(spark, date, targetApp).map(x => (x._1, x._2.distinct.mkString(","))).toDF("uid", "appNames")
    baseData.write.mode("overwrite").saveAsTable("test.app_count_sjq")
  }

  def getBaseData(spark: SparkSession, date: String, targetApp: String) = {
    import spark.implicits._
    val sqlRequest =
      s"""
         |select
         | uid,
         | concat_ws(',', app_name) as pkgs
         | from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin

    println(sqlRequest)
    val t1 = new Date()
    println("T1 is " + t1)

    val df0 = spark.sql(sqlRequest)
    val countUpper = df0.count()*0.2

    val df1 = df0.rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs").split(",")))
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
        if (arr.size == 2) (uid, arr(1)) else (uid, "")
      }).toDF("uid", "appName").persist()

    val t2 = new Date()
    println("T2 is " + t2)

    val df20 = df1.rdd
      .map(x => x.getAs[String]("appName"))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).toDF("appName", "count")

    val countLower = df20.where(s"appName = '${targetApp}'").select("count").rdd.map(x => x.getAs[Int]("count")).reduce(_ + _)
    val filter_condition = s"appName not like '%小米%' and appName not like '%OPPO%' and  count between ${countLower} and ${countUpper}"
    println(filter_condition)
    val df2 = df20.filter(filter_condition)
    df20.write.mode("overwrite").saveAsTable("test.appCount_sjq")

    val t3 = new Date()
    println("T3 is " + t3)
    //==========================================================================
    val A = df1.where(s"appName = '${targetApp}'").select("uid").distinct() //含targetApp的uid的集合
    val B = df1.join(A, Seq("uid")).select("appName").distinct() //A安装的appName的集合
//    val C = df1.join(B, Seq("appName")).select("uid").distinct() //安装B中appName的uid的集合
    //===========================================================================

    val df3 = df1.join(B, Seq("appName")).join(df2, Seq("appName") ).select("uid", "appName", "count").filter("count is not NULL")

    //    df3.write.mode("overwrite").saveAsTable("test.app_count_sjq")

    val t4 = new Date()
    println("T4 is " + t4)

    val result = df3.rdd
      .map(x => (x.getAs[String]("uid"), Array(x.getAs[String]("appName"))))
      .reduceByKey((appArr1, appArr2) => appArr1 ++ appArr2)
    val t5 = new Date()
    println("T5 is " + t5)
    result
  }

  case class UidComb(var uid: String, var comb: String)

}








