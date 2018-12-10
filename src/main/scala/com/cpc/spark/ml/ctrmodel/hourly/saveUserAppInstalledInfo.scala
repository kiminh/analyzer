package com.cpc.spark.ml.ctrmodel.hourly

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import userprofile.Userprofile.{APPPackage, UserProfile}
import org.apache.log4j.{Level, Logger}


/**
  * Created by zhaolei on 22/12/2017.
  */

object saveUserAppInstalledInfo {

  Logger.getRootLogger.setLevel(Level.WARN)

  var tableName = ""

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: save user installed apps <date=string> <hour=string>
           |
        """.stripMargin
      )
      System.exit(1)
    }

    val date = args(0)
    tableName = args(1)

    val spark = SparkSession.builder().appName("save user installed apps " + date)
      .enableHiveSupport().getOrCreate()

    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(date)
    val outpath = "/user/cpc/userInstalledApp/%s".format(date)

    println("------save user installed apps %s------".format(date))

    import spark.implicits._
    var pkgs = spark.read.orc(inpath)
      .rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, (x._2.distinct, 1)))
      .cache()


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
    val sum = added.coalesce(200).map(x => (x._1, x._2._1))
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

    //保存当天的数据
    added.map(x => (x._1, x._2._1))
      .toDF("uid", "pkgs")
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet(outpath)

    val sql =
      """
        |ALTER TABLE dl_cpc.cpc_user_installed_apps add if not exists PARTITION (load_date = "%s" )  LOCATION
        |       '/user/cpc/userInstalledApp/%s'
        |
                """.stripMargin.format(date, date)
    spark.sql(sql)

    pkgs.unpersist()
    spark.close()
  }

  def updateUserPkgDict(spark: SparkSession, pkgs: RDD[(String, List[String])]): Unit = {
    import spark.implicits._
    val userPkgs = userPkgDict().toDF("pkg_name", "id")
    val topPkgs = pkgs.flatMap(x => x._2.map(v => (v, 1)))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(1000)
      .toSeq
      .toDF("pkg_name", "user_num")

    val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
    var n1 = 0
    var n2 = 0
    val conf = ConfigFactory.load()
    Class.forName(conf.getString("mariadb.driver"))
    val conn = DriverManager.getConnection(
      conf.getString("mariadb.url"),
      conf.getString("mariadb.user"),
      conf.getString("mariadb.password"))
    topPkgs.join(userPkgs, Seq("pkg_name"), "leftouter")
      .rdd
      .toLocalIterator
      .foreach {
        row =>
          val id = row.getAs[Int]("id")
          val name = row.getAs[String]("pkg_name")
          val num = row.getAs[Int]("user_num")
          if (id > 0) {
            val stmt = conn.createStatement()
            val sql =
              """
                |update %s set `user_num` = %d, `update_time` = "%s" where `id` = %d
              """.stripMargin.format(tableName, num, dateStr, id)
            val rs = stmt.execute(sql)
            n1 += 1
          } else {
            val stmt = conn.createStatement()
            val sql =
              """
                |insert into %s (`pkg_name`, `user_num`, `create_time`, `update_time`)
                | values ("%s", %d, "%s", "%s")
              """.stripMargin.format(tableName, name, num, dateStr, dateStr)
            val rs = stmt.execute(sql)
            n2 += 1
          }
      }

    println("update", n1, "add", n2)
  }

  def userPkgDict(): Seq[(String, Int)] = {
    val conf = ConfigFactory.load()
    Class.forName(conf.getString("mariadb.driver"))
    val conn = DriverManager.getConnection(
      conf.getString("mariadb.url"),
      conf.getString("mariadb.user"),
      conf.getString("mariadb.password"))

    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("select * from %s".format(tableName))
    var pkgs = Seq[(String, Int)]()
    while (rs.next()) {
      pkgs = pkgs :+ (rs.getString("pkg_name"), rs.getInt("id"))
    }
    pkgs
  }
}

