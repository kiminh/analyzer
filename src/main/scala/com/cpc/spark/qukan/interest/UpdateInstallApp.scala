package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import com.cpc.spark.streaming.tools.{Encoding, Gzip}
import com.cpc.spark.common.Utils
import org.apache.spark.rdd.RDD


/**
  * Created by YuntaoMa on 06/06/2018.
  */

object UpdateInstallApp {
  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder()
      .appName("update cpc install app")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -days)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val qukanApps = spark.read.parquet("/user/cpc/userInstalledApp/%s".format(date)).rdd.map {
      r =>
        val uid = r.getAs[String]("uid")
        val pkgs = r.getAs[Seq[String]]("pkgs")
        (uid, (Seq[String](), Seq[String](), Seq[String](), pkgs))
    }
    println("origin statistic")
    println(qukanApps.count())
    println(qukanApps.filter(_._2._3.length > 10).count())

    val stmt =
      """
        |select trace_op1, trace_op2, trace_op3 from dl_cpc.cpc_all_trace_log where `date` = "%s" and trace_type = "%s"
      """.stripMargin.format(date, "app_list")
    println(stmt)
    val all_list = spark.sql(stmt).rdd.map {
      r =>
        val op_type = r.getAs[String](0)
        val did = r.getAs[String](1)
        val in_b64 = r.getAs[String](2)
        var in : String = ""
        var apps = Seq[String]()
        if (in_b64 != null) {
          val in_gzip = com.cpc.spark.streaming.tools.Encoding.base64Decoder(in_b64).toArray
          in = Gzip.decompress(in_gzip) match {
            case Some(s) => s
            case None => null
          }
          if (in != null) {
              apps = for {
              JArray(pkgs) <- parse(in)
              JObject(pkg) <- pkgs
              JField("name", JString(name)) <- pkg
              JField("package_name", JString(package_name)) <- pkg
              p = (package_name)
            } yield p
          }
        }
        if (op_type == "APP_LIST_ADD") {
          (did, (apps, Seq(), Seq(), Seq()))
        } else if (op_type == "APP_LIST_REMOVE") {
          (did, (Seq(), apps, Seq(), Seq()))
        } else if (op_type == "APP_LIST_USE"){
          (did, (Seq(), Seq(), apps, Seq()))
        } else if (op_type == "APP_LIST_INSTALLED") {
          (did, (Seq(), Seq(), Seq(), apps))
        } else {
          null
        }
    }.filter(_ != null)
      .union(qukanApps)
      .reduceByKey((x, y) => ((x._1 ++ y._1).distinct, (x._2 ++ y._2).distinct, (x._3 ++ y._3).distinct, (x._4 ++ y._4).distinct))



        //.map(x => (x._1, x._2._1.distinct, x._2._2.distinct, x._2._3.distinct))
    all_list.take(10).foreach(println)
    println(all_list.count())
    println(all_list.filter(x => x._2._4.length > 5).count())
    println(all_list.filter(x => x._2._4.length > 10).count())
    all_list.map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4)).toDF("uid", "add_pkgs", "remove_pkgs", "used_pkgs", "pkgs").write.mode(SaveMode.Overwrite).parquet("/user/cpc/traceInstalledApp/%s".format(date))


  }

}