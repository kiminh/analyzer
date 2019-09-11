package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.qukan.parser.HdfsParser
import com.cpc.spark.streaming.tools.Gzip
import com.cpc.spark.streaming.tools.Encoding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import redis.clients.jedis.{HostAndPort, JedisCluster}
import userprofile.Userprofile.{APPPackage, UserProfileV2}


/**
  * Created by YuntaoMa on 06/06/2018.
  *
  * new owner: Yao Xiong (18/03/2019).
  * new owner: Yiming Fan (06/09/2019).
  */

object UpdateInstallApp {
  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder().appName("update cpc install app").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val today = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -days)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    //    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(yesterday)
    val outpath = "hdfs://emr-cluster/user/cpc/userInstalledApp/%s".format(date) // 增加绝对路径

    val stmt =
      """
        |select trace_op1, trace_op2, trace_op3, opt from dl_cpc.cpc_basedata_trace_event
        |where `day` = "%s" and trace_type = "%s"
      """.stripMargin.format(date, "app_list")
    println(stmt)
    val all_list = spark.sql(stmt).rdd.map {
      r =>
        val op_type = r.getAs[String](0)
        val did = r.getAs[String](1) // 可以认为 did(trace_op2) 就是uid
        val in_b64 = r.getAs[String](2)
        val opt = r.getMap[String, String](3)
        var in : String = ""
        var apps = Seq[String]()
        var names = Seq[String]()
        var name_pkg = Seq[(String, String)]()
        var ops = Seq[(String)]()
        var valid = true
        if (in_b64 != null) {
          val in_gzip = Encoding.base64Decoder(in_b64).toArray
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
        var opt_app_package_name :String=""

        if (opt != null){
          if (opt.contains("app_package_name")){
            opt_app_package_name = opt("app_package_name")
          }
        }

        // 增加tuid逻辑
        var tuid : String = ""
        if (opt != null) {
          if (opt.contains("app_tuid")) {
            tuid = opt("app_tuid")
          }
        }

        ops =  ops :+ (opt_app_package_name)
        apps = name_pkg.map{x => x._1}
        names = name_pkg.map{x => x._1 + "-" + x._2}

        if (op_type == "APP_LIST_ADD") {
          (did, (apps, Seq[String](), Seq[String](), apps.toList, Seq[String](), ops, tuid))
        } else if (op_type == "APP_LIST_REMOVE") {
          (did, (Seq[String](), apps, Seq[String](), List[String](), Seq[String](), ops, tuid))
        } else if (op_type == "APP_LIST_USE"){
          (did, (Seq[String](), Seq[String](), apps, List[String](), Seq[String](), ops, tuid))
        } else if (op_type == "APP_LIST_INSTALLED") {
          (did, (Seq[String](), Seq[String](), Seq[String](), apps.toList, names, ops, tuid))
        } else {
          null
        }
    }.filter(_ != null)
      //      .union(qukanApps)
      .reduceByKey {
        (x, y) => (
          (x._1 ++ y._1).distinct, // apps 包名
          (x._2 ++ y._2).distinct, // 删除的包名
          (x._3 ++ y._3).distinct, // 使用的包名
          (x._4 ++ y._4).distinct, // app list
          (x._5 ++ y._5).distinct, // 包名-app名
          (x._6 ++ y._6).distinct, // opt_app_package_name
          x._7 // tuid
        )
      }


    //    var old: RDD[(String, (List[String], Int))] = null
    //    try {
    //      old = spark.read
    //        .parquet(outpath)
    //        .rdd
    //        .map {
    //          x =>
    //            (x.getAs[String]("uid"), (x.getAs[Seq[String]]("pkgs").toList, 0))
    //        }
    //      println("old", old.count())
    //    } catch {
    //      case e: Exception =>
    //        println("old", 0)
    //    }
    //
    //    //标记出老数据
    //    var pkgs = all_list.map(x => (x._1, (x._2._4, 1)))
    //    if (old != null) {
    //      pkgs = pkgs.union(old)
    //        .reduceByKey {
    //          (x, y) =>
    //            if (x._2 == 0) {
    //              x
    //            } else {
    //              y
    //            }
    //        }
    //    }
    //    val added = pkgs.filter(_._2._2 == 1)
    //    added.map(x => (x._1, x._2._1)).take(10).foreach(println)
    //
    //
    //    var count = added.count()
    //    println("data count:", count)
    //    if (days == 0 && count < 10) {
    //      println(s"data count below threshold: $count")
    //      throw new RuntimeException(s"data count below threshold: $count for time in $date")
    //    }
    //    //修复昨天丢失数据
    //    if (days == 1 && count > 0){
    //      println(s"fix yesterday app data  $count")
    //    }
    //
    //    //新增数据迁移至新的redis集群
    //    val result = added.map(x => (x._1, x._2._1))
    //      .repartition(100)
    //      .mapPartitions {
    //        p =>
    //          var n1 = 0
    //          var n2 = 0
    //          var matchedKey = 0
    //          val redisV2 = new JedisCluster(new HostAndPort("192.168.86.36", 7103))
    //          val sec = new Date().getTime / 1000
    //          p.foreach {
    //            x =>
    //              val key = x._1 + "_upv2"
    //              val buffer = redisV2.get(key.getBytes)
    //              var userV2: UserProfileV2.Builder = null
    //              try {
    //                if (buffer == null) {
    //                  userV2 = UserProfileV2.newBuilder()
    //                } else {
    //                  userV2 = UserProfileV2.parseFrom(buffer).toBuilder
    //                }
    //              }catch {
    //                case e: Exception => println(e)
    //              }
    //              //判断老数据
    //              if (userV2 != null){
    //                if (userV2.getInstallpkgCount > 0) {
    //                  val pkg = userV2.getInstallpkg(0)
    //                  //更新时间大于一天
    //                  if (sec > pkg.getLastUpdateTime + 60 * 60 * 24) {
    //                    userV2.clearInstallpkg()
    //                  } else {
    //                    n1 += 1
    //                  }
    //                }
    //                if (userV2.getInstallpkgCount == 0) {
    //                  x._2.foreach {
    //                    n =>
    //                      val pkg = APPPackage.newBuilder().setPackagename(n).setLastUpdateTime(sec)
    //                      userV2.addInstallpkg(pkg)
    //                  }
    //                  if (userV2.build.getInstallpkgCount() > 0){
    //                    matchedKey += 1
    //                  }
    //                  var result =  redisV2.setex(key.getBytes, 3600 * 24 * 14, userV2.build().toByteArray)
    //                  if (result.equals("OK")){
    //                    n2 += 1
    //                  }
    //                }
    //              }
    //
    //          }
    //          redisV2.close()
    //          Seq(("new", n1), ("update", n2),("matched", matchedKey)).iterator
    //      }
    //      .reduceByKey(_ + _)
    //      .take(10)
    //    println("update to new redis:")
    //    result.foreach(println)

    all_list.map(x => (x._1, x._2._4, x._2._1, x._2._2, x._2._3, x._2._5, "", "", "", "", x._2._7, x._2._6))
      .toDF("uid", "pkgs", "add_pkgs", "remove_pkgs", "used_pkgs", "app_name","opt_app_package_name",
        "opt_app_name","opt_app_version", "opt_app_tkid","opt_app_tuid","opt_app_package_name_new")
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.fym_installed_tuid_600")
    //      .parquet("hdfs://emr-cluster/user/cpc/userInstalledApp/%s".format(date))
    //    val sql =
    //      """
    //        |ALTER TABLE dl_cpc.cpc_user_installed_apps
    //        |add if not exists PARTITION (load_date = '%s')
    //        |LOCATION 'hdfs://emr-cluster/user/cpc/userInstalledApp/%s'
    //      """.stripMargin.format(date, date)
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


  }

}