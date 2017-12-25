package com.cpc.spark.ml.ctrmodel.v1

import java.sql.DriverManager

import com.cpc.spark.qukan.parser.HdfsParser
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by zhaolei on 22/12/2017.
  */

object SaveUserAppInstalledInfo{

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

    //保存当天的
    pkgs.map(x => (x._1, x._2.distinct.mkString(",")))
      .toDF("uid", "pkgs")
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outpath)

    //更新字典
    val usedPkgs = userPkgDict()
    val topPkgs = pkgs.flatMap(x => x._2.map(v => (v, 1)))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(1000)
      .filter(x => !usedPkgs.contains(x._1))

    println("pkg num", usedPkgs.length, topPkgs.length)
    updateUserPkgDict(topPkgs)

    spark.close()
  }

  def updateUserPkgDict(topPkgs: Seq[(String, Int)]): Unit = {
    val conf = ConfigFactory.load()
    Class.forName(conf.getString("mariadb.driver"))
    val conn = DriverManager.getConnection(
      conf.getString("mariadb.url"),
      conf.getString("mariadb.user"),
      conf.getString("mariadb.password"))

    topPkgs.foreach {
      x =>
        val stmt = conn.createStatement()
        val sql = "insert into %s (`pkg_name`, `user_num`) values (\"%s\", %d)".format(tableName, x._1, x._2)
        val rs = stmt.execute(sql)
    }
  }

  def userPkgDict(): Seq[String] = {
    val conf = ConfigFactory.load()
    Class.forName(conf.getString("mariadb.driver"))
    val conn = DriverManager.getConnection(
      conf.getString("mariadb.url"),
      conf.getString("mariadb.user"),
      conf.getString("mariadb.password"))

    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("select * from %s".format(tableName))
    var pkgs = Seq[String]()
    while (rs.next()) {
      pkgs = pkgs :+ rs.getString("pkg_name")
    }
    pkgs
  }
}

