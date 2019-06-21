package com.cpc.spark.report

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.qukan.parser.HdfsParser
import com.cpc.spark.streaming.tools.Gzip
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import redis.clients.jedis.{HostAndPort, JedisCluster}
import userprofile.Userprofile.{APPPackage, UserProfileV2}


/**
  * Created by YuntaoMa on 06/06/2018.
  */

object UpdateInstallAppTest {
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
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(date)
    val outpath = "/user/cpc/userInstalledApp/%s".format(date)

    var qukanApps = spark.read.orc(inpath)
      .rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, (Seq[String](), Seq[String](), Seq[String](), x._2.distinct, Seq[String]())))

    println("origin statistic")
    println(qukanApps.count())
    println(qukanApps.filter(_._2._3.length > 10).count())
    println(qukanApps.filter(_._2._4.contains("com.jifen.qukan")).count())

    val stmt =
      """
        |select trace_op1, trace_op2, trace_op3 from dl_cpc.cpc_basedata_trace_event
        |where `day` = "%s" and trace_type = "%s"
      """.stripMargin.format(date, "app_list")
    println(stmt)
    val all_list = spark.sql(stmt).rdd.map {
      r =>
        val op_type = r.getAs[String](0)
        val did = r.getAs[String](1)
        val in_b64 = r.getAs[String](2)
        var in : String = ""
        var apps = Seq[String]()
        var names = Seq[String]()
        var name_pkg = Seq[(String, String)]()
        var valid = true
        if (in_b64 != null) {
          val in_gzip = com.cpc.spark.streaming.tools.Encoding.base64Decoder(in_b64).toArray
          in = Gzip.decompress(in_gzip) match {
            case Some(s) => s
            case None => null
          }
          if (in != null) {
            try{
              name_pkg = for {
                JArray(pkgs) <- parse(in)
                JObject(pkg) <- pkgs
                JField("name", JString(name)) <- pkg
                JField("package_name", JString(package_name)) <- pkg
                p = (package_name, name)
              } yield p
            } catch {
              case e: Exception => null
            }
          }
        }

        apps = name_pkg.map{x => x._1}
        names = name_pkg.map{x => x._1 + "-" + x._2}

        if (op_type == "APP_LIST_ADD") {
          (did, (apps, Seq[String](), Seq[String](), apps.toList, Seq[String]()))
        } else if (op_type == "APP_LIST_REMOVE") {
          (did, (Seq[String](), apps, Seq[String](), List[String](), Seq[String]()))
        } else if (op_type == "APP_LIST_USE"){
          (did, (Seq[String](), Seq[String](), apps, List[String](), Seq[String]()))
        } else if (op_type == "APP_LIST_INSTALLED") {
          (did, (Seq[String](), Seq[String](), Seq[String](), apps.toList, names))
        } else {
          null
        }
    }.filter(_ != null)
      .union(qukanApps)
      .reduceByKey{(x, y) =>
        ((x._1 ++ y._1).distinct, (x._2 ++ y._2).distinct, (x._3 ++ y._3).distinct,
          (x._4 ++ y._4).distinct, (x._5 ++ y._5).distinct)
      }


    println("all_list-------------")
    val app= all_list.map(x => (x._1, x._2._4, x._2._1, x._2._2, x._2._3, x._2._5))
      .toDF("uid", "pkgs", "add_pkgs", "remove_pkgs", "used_pkgs", "app_name")

    println("app"+app.count())
    println("qtt app: "+app.where("array_contains(pkgs, 'com.jifen.qukan')").count())




  }

}