package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import com.cpc.spark.streaming.tools.{Encoding, Gzip}
import com.cpc.spark.common.Utils
import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import userprofile.Userprofile.{APPPackage, UserProfile}


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
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(date)
    val outpath = "/user/cpc/traceInstalledApp/%s".format(date)

    var qukanApps = spark.read.orc(inpath)
      .rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, (Seq[String](), Seq[String](), Seq[String](), for (i <- x._2.distinct)yield (Seq(i)))))

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
          (did, (apps, Seq[String](), Seq[String](), Seq[String]()))
        } else if (op_type == "APP_LIST_REMOVE") {
          (did, (Seq[String](), apps, Seq[String](), Seq[String]()))
        } else if (op_type == "APP_LIST_USE"){
          (did, (Seq[String](), Seq[String](), apps, Seq[String]()))
        } else if (op_type == "APP_LIST_INSTALLED") {
          (did, (Seq[String](), Seq[String](), Seq[String](), apps))
        } else {
          null
        }
    }.filter(_ != null)
      .union(qukanApps)
      .reduceByKey((x, y) => ((x._1 ++ y._1).distinct, (x._2 ++ y._2).distinct, (x._3 ++ y._3).distinct, (x._4 ++ y._4).distinct))

    println(all_list.map(x => (x._2._1.length, x._2._2.length, x._2._3.length, x._2._4.length))
        .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)))

    all_list.flatMap(x => x._2._3).map{x => (x, 1)}.reduceByKey(_+_).sortBy(_._2, false)
        .take(50).foreach(println)


    all_list.take(10).foreach(println)
    println(all_list.count())
    println(all_list.filter(x => x._2._4.length > 5).count())
    println(all_list.filter(x => x._2._4.length > 10).count())
    all_list.map(x => (x._1, x._2._4, x._2._1, x._2._2, x._2._3, date)).toDF("uid", "pkgs", "add_pkgs", "remove_pkgs", "used_pkgs", "load_date").write.mode(SaveMode.Overwrite).parquet("/user/cpc/traceInstalledApp/%s".format(date))

//    val sql =
//      """
//        |ALTER TABLE dl_cpc.cpc_user_installed_apps add if not exists PARTITION (load_date = "%s" )  LOCATION
//        |       '/user/cpc/userInstalledApp/%s'
//        |
//                """.stripMargin.format(date, date)
//    spark.sql(sql)

//    val yest = spark.read.parquet("/user/cpc/traceInstalledApp/%s".format(yesterday)).rdd.map {
//      r =>
//        val did = r.getAs[String](0)
//        val use = r.getAs[Seq[String]](3)
//        (did, use)
//    }.join(all_list.map(x => (x._1, x._2._3)))
//        .map {
//          x =>
//            ((x._2._1.toSet[String] -- x._2._2.toSet[String]), (x._2._2.toSet[String] -- x._2._1.toSet[String]))
//        }
//    println(yest.count())
//    println(yest.map {
//      x =>
//        (x._1.size, x._2.size)
//    }.reduce((x, y) => (x._1 + y._1, x._2 + y._2)))

    var old: RDD[(String, (List[String], Int))] = null
    try {
      old = spark.read
        .parquet(outpath)
        .rdd
        .map {
          x =>
            (x.getAs[String]("uid"), (x.getAs[Seq[String]]("pkgs").toList, 0))
        }
      println("old", old.count())
    } catch {
      case e: Exception =>
        println("old", 0)
    }

    //标记出老数据
    var pkgs = all_list.map(x => (x._1, (x._2._4.toList, 1)))
    if (old != null) {
      pkgs = pkgs.union(old)
        .reduceByKey {
          (x, y) =>
            if (x._2 == 0) {
              x
            } else {
              y
            }
        }
    }
    val added = pkgs.filter(_._2._2 == 1)
    println("new", added.count())

    //保存新增数据 redis
    val sum = added.map(x => (x._1, x._2._1))
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          var n2 = 0
          var n3 = 0
          val conf = ConfigFactory.load()
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          val sec = new Date().getTime / 1000
          p.foreach {
            x =>
              n += 1
              val key = x._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)

              var user: UserProfile.Builder = null
              if (buffer == null) {
                user = UserProfile.newBuilder()
                n3 = n3 + 1
              } else {
                user = UserProfile.parseFrom(buffer).toBuilder
              }

              //判断老数据
              if (user.getInstallpkgCount > 0) {
                val pkg = user.getInstallpkg(0)
                //更新时间大于一天
                if (sec > pkg.getLastUpdateTime + 60 * 60 * 24) {
                  user.clearInstallpkg()
                } else {
                  n1 += 1
                }
              }

              if (user.getInstallpkgCount == 0) {
                x._2.foreach {
                  n =>
                    val pkg = APPPackage.newBuilder().setPackagename(n).setLastUpdateTime(sec)
                    user.addInstallpkg(pkg)
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                n2 += 1
              }
          }
          Seq(("pass", n1), ("update", n2), ("new", n3)).iterator
      }
      .reduceByKey(_ + _)
      .take(10)
    println("update redis")
    sum.foreach(println)
  }

}