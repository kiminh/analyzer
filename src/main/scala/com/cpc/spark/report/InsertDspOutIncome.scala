package com.cpc.spark.report

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created on 2018-12-12 16
  */
object InsertDspOutIncome {

  var mariadbUrl = ""
  val mariadbProp = new Properties()
  var day = ""
  var table = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    day = args(0).toString
    table = args(1).toString

    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = ConfigFactory.load()
    /*mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))*/

    mariadbUrl = "jdbc:mysql://rm-2zef52mz0p6mv5007.mysql.rds.aliyuncs.com:3306/union_test"
    mariadbProp.put("user", "cpcrw")
    mariadbProp.put("password", "zZdlz9qUB51awT8b")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")

    val spark = SparkSession.builder()
      .appName("insert dsp_out_income date " + day)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    Class.forName(mariadbProp.getProperty("driver"))
    val conn = DriverManager.getConnection(
      mariadbUrl,
      mariadbProp.getProperty("user"),
      mariadbProp.getProperty("password"))

    val sql2 = "show tables"
    val stmt = conn.createStatement()
    val resultSet = stmt.executeQuery(sql2)
    while (resultSet.next()) {
      println("@@@@@: " + resultSet.getString(1))
    }

    val sql =
      s"""
         |SELECT
         | `date`,
         |  ext_string["dsp_adslotid_by_src_22"] as dsp_adslot_id,
         |  sum(
         |    CASE
         |      WHEN isshow == 1 THEN price/1000
         |      ELSE 0
         |    END
         |  ) AS dsp_income,
         |  sum(isclick) as dsp_click,
         |  sum(isshow) as dsp_impression
         |FROM
         |  dl_cpc.cpc_union_log
         |WHERE
         |  adsrc = 22
         |  AND isshow = 1
         |  AND `date` = "$day"
         |GROUP BY
         |  `date`,
         |  ext_string["dsp_adslotid_by_src_22"]
       """.stripMargin
    println("sql: " + sql)

    var dspLog = spark.sql(sql)
      .rdd
      .map {
        r =>
          val dsp_adslot_id = r.getAs[String]("dsp_adslot_id")
          val dsp_income = r.getAs[Double]("dsp_income")
          val dsp_click = r.getAs[Long]("dsp_click")
          val dsp_impression = r.getAs[Long]("dsp_impression")
          (dsp_adslot_id, dsp_impression, dsp_click, dsp_income)
      }

    dspLog.take(10).foreach(x => println("##: " + x._1 + ", " + x._2 + ", " + x._3 + ", " + x._4))
    dspLog.foreachPartition(updateData)

    /*for (log <- dspLog) {
     val dsp_adslot_id = log.getAs[String]("dsp_adslot_id")
     val dsp_income = log.getAs[Double]("dsp_income")
     val dsp_click = log.getAs[Long]("dsp_click")
     val dsp_impression = log.getAs[Long]("dsp_impression")
     val n = updateData(table, day, dsp_adslot_id, dsp_income, dsp_click, dsp_impression)
   }*/
    //    val s = Seq()

    //    dspLog.foreach { r =>
    //      val dsp_adslot_id = r.getAs[String]("dsp_adslot_id")
    //      val dsp_income = r.getAs[Double]("dsp_income")
    //      val dsp_click = r.getAs[Long]("dsp_click")
    //      val dsp_impression = r.getAs[Long]("dsp_impression")
    //      val n = updateData(table, day, dsp_adslot_id, dsp_income, dsp_click, dsp_impression)
    //      s :+ r
    //    }
    //    println("return: " + s)
    println("~~~~~~write to mysql successfully")

  }

  /*def updateData(table: String, day: String, dsp_adslot_id: String, dsp_income: Double, dsp_click: Long, dsp_impression: Long): Int = {
    println("#####: " + table + ", " + day + ", " + dsp_adslot_id + ", " + dsp_income + ", " + dsp_click + ", " + dsp_impression)
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        s"""
           |update union_test.%s
           |set dsp_income = %s,
           |dsp_click = %s,
           |dsp_impression = %s
           |where `date` = "%s" and dsp_adslot_id = "%s" and ad_src = 22
      """.stripMargin.format(table, dsp_income, dsp_click, dsp_impression, day, dsp_adslot_id)
      println("sql" + sql);
      val num = stmt.executeUpdate(sql);
      num
    } catch {
      case e: Exception => println("exception caught: " + e)
        -1
    }
  }*/

  def updateData(iter: Iterator[(String, Long, Long, Double)]): Unit = {
    var conn: Connection = null
    var stmt: Statement = null
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      conn.setAutoCommit(false)
      stmt = conn.createStatement()

      iter.foreach { x =>
        /*val dsp_adslot_id = r.getAs[String]("dsp_adslot_id")
        val dsp_income = r.getAs[Double]("dsp_income")
        val dsp_click = r.getAs[Long]("dsp_click")
        val dsp_impression = r.getAs[Long]("dsp_impression")*/

        val sql =
          s"""
             |update union_test.%s
             |set dsp_income = %s,
             |dsp_click = %s,
             |dsp_impression = %s
             |where `date` = "%s" and dsp_adslot_id = "%s" and ad_src = 22
        """.stripMargin.format(table, x._4, x._3, x._2, day, x._1)
        println("sql" + sql);
        stmt.executeUpdate(sql)

      }
    } catch {
      case e: Exception => println("exception caught: " + e)
    } finally {
      if (conn != null) {
        conn.close()
      }
      if (stmt != null) {
        stmt.close()
      }
    }
  }


}
