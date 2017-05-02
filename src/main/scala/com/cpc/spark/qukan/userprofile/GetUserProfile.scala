package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * Created by Roy on 2017/4/14.
  */
object GetUserProfile {

  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <hdfs_input> <hdfs_ouput> <hour_before>
           |
        """.stripMargin)
      System.exit(1)
    }

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

    val ctx = SparkSession.builder()
      .appName("cpc get user profile [%s]".format(day))
      .getOrCreate()

    //user app install info
    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val aiRdd = ctx.read.orc(aiPath).rdd
    val aiData = aiRdd.take(10).foreach {
      x =>
        println(x.getString(0), x.getString(1), x.getString(2))
    }

    System.exit(1)

    /*
    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    val profileRdd = ctx.read.text(profilePath).rdd
    val pData = profileRdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(x => x != null && x.getDevid.length > 0)


      //.map(x => HdfsParser.parseAppInstall(x))
      //.filter(x => x != null && x.getDevid.length > 0)


    for (n <- 0 to 14) {
      val path = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s/%06d*".format(day, n)
      val ctx = sc.textFile(path)
      val result = ctx.map(row => HdfsParser.parseTextRow(row))
        .filter(x => x.ok)
        .map(x => (x.devid, x))
        .reduceByKey((x, y) => y)
        .map(x => x._2)

      result.collect().foreach {
        row =>
          val devid = row.devid
          val profile = Userprofile
            .UserProfile
            .newBuilder()
            .setAge(row.age)
            .setSex(row.sex)
            .setCoin(row.coin)
            .build()
          redis.set(devid, profile.toByteArray)
      }
    }

    sc.stop()
    */

    ctx.stop()
  }
}

