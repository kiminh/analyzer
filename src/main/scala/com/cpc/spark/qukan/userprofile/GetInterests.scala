package com.cpc.spark.qukan.userprofile

import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{APPPackage, InterestItem, UserProfile}
import com.redis.serialization.Parse.Implicits._

/**
  * Created by Roy on 2017/5/17.
  */
object GetInterests {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before> <int>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = HdfsParser.dateFormat.format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val allowedPkgs = conf.getStringList("userprofile.allowed_pkgs")
    val pkgTags = conf.getConfig("userprofile.pkg_tags")

    val ctx = SparkSession.builder()
      .appName("cpc get user interests [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
    val aiRdd = ctx.read.orc(aiPath).rdd
      .map(HdfsParser.parseInstallApp(_, x => allowedPkgs.contains(x), pkgTags))
      .filter(x => x != null && x.devid.length > 0)

    var intrn = 0
    aiRdd.map(x => (x.devid, x))
      .toLocalIterator
      .foreach {
        x =>
          val row = x._2
          val key = row.devid + "_UPDATA"
          val buffer = redis.get[Array[Byte]](key).getOrElse(null)
          if (buffer != null) {
            val user = UserProfile.parseFrom(buffer).toBuilder
            row.uis.foreach {
              ui =>
                val i = InterestItem
                  .newBuilder()
                  .setTag(ui.tag)
                  .setScore(ui.score)
                  .build()
                user.addInterests(i)
            }

            row.pkgs.foreach {
              p =>
                val pkg = APPPackage
                  .newBuilder()
                  .setFirstInstallTime(p.firstInstallTime)
                  .setLastUpdateTime(p.lastUpdateTime)
                  .setPackagename(p.name)
                  .build()
                user.addInstallpkg(pkg)
            }

            intrn = intrn + 1
            redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
          }
      }
    println("count", intrn)
    ctx.stop()
  }

}
