package com.cpc.spark.ml.ctrmodel.hourly

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import userprofile.Userprofile.{APPPackage, UserProfile}


/**
  * Created by zhaolei on 22/12/2017.
  */

object saveUserAppInstalledInfo{

  var tableName = ""

  def main(args:Array[String]): Unit ={
    if(args.length < 1){
      System.err.println(
        s"""
           |Usage: Tag user by installed apps <dayBefore int>
           |
        """.stripMargin
      )
      System.exit(1)
    }

    val date = args(0)
    tableName = args(1)

    val spark = SparkSession.builder().appName("save user installed apps" + date)
      .enableHiveSupport().getOrCreate()

    val inpath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(date)
    val outpath = "/user/cpc/userInstalledApp/%s".format(date)

    println("------save user installed apps %s------".format(date))

    import spark.implicits._
    val pkgs = spark.read.orc(inpath)
      .rdd
      .map(HdfsParser.parseInstallApp(_, x => true, null))
      .filter(x => x != null && x.pkgs.length > 0)
      .map(x => (x.devid, x.pkgs.map(_.name)))
      .reduceByKey(_++_)
      .map(x => (x._1, x._2.distinct))

    //保存当天的
    pkgs.toDF("uid", "pkgs")
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outpath)

    //保存redis
    val sum = pkgs
      .mapPartitions {
        p =>
          var n1 = 0
          var n2 = 0
          val conf = ConfigFactory.load()
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            x =>
              n1 = n1 + 1
              val key = x._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                user.clearInstallpkg()
                x._2.foreach {
                  n =>
                    val pkg = APPPackage.newBuilder().setPackagename(n)
                    user.addInstallpkg(pkg)
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                n2 += 1
              }
          }
          Seq(("all", n1), ("update", n2)).iterator
      }
      .reduceByKey(_ + _)
      .take(10)
    println("update redis")
    sum.foreach(println)

    //更新字典
    //updateUserPkgDict(spark, pkgs)

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

