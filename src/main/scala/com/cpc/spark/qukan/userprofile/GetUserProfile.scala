package com.cpc.spark.qukan.userprofile

import java.util.Calendar

import com.cpc.spark.qukan.parser.{HdfsParser, ProfileRow}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{APPPackage, InterestItem, UserProfile}

/**
  * Created by Roy on 2017/4/14.
  */
object GetUserProfile {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before>
           |
        """.stripMargin)
      System.exit(1)
    }

    val dayBefore = args(0).toInt

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = HdfsParser.dateFormat.format(cal.getTime)

    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    val allowedPkgs = conf.getStringList("userprofile.allowed_pkgs")
    val pkgTags = conf.getConfig("userprofile.pkg_tags")

    val ctx = SparkSession.builder()
      .appName("cpc get user profile [%s]".format(day))
      .getOrCreate()

    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    var unionRdd = ctx.read.text(profilePath).rdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(x => x != null && x.devid.length > 0)

    //user app install info
    for (d <- 0 to dayBefore - 1) {
      val day = HdfsParser.dateFormat.format(cal.getTime)
      val aiPath = "/gobblin/source/lechuan/qukan/extend_report/%s".format(day)
      val aiRdd = ctx.read.orc(aiPath).rdd
        .map(HdfsParser.parseInstallApp(_, x => allowedPkgs.contains(x), pkgTags))
        .filter(x => x != null && x.devid.length > 0)
      unionRdd = unionRdd.union(aiRdd)
      cal.add(Calendar.DATE, -1)
    }

    unionRdd.map(x => (x.devid, x))
      .reduceByKey {
        (x, y) =>
          if (x.from == 0) {
            merge(x, y)
          } else {
            merge(y, x)
          }
      }
      .map(_._2)
      .toLocalIterator
      .foreach {
        x =>
          val profile = UserProfile
            .newBuilder()
            .setDevid(x.devid)
            .setAge(x.age)
            .setSex(x.sex)
            .setCoin(x.coin)

          x.pkgs.foreach {
            p =>
              val pkg = APPPackage
                .newBuilder()
                .setFirstInstallTime(p.firstInstallTime)
                .setLastUpdateTime(p.lastUpdateTime)
                .setPackagename(p.name)
                .build()
              profile.addInstallpkg(pkg)
          }

          x.uis.foreach {
            ui =>
              val i = InterestItem
                .newBuilder()
                .setTag(ui.tag)
                .setScore(ui.score)
                .build()
              profile.addInterests(i)
          }

          redis.setex(x.devid + "_UPDATA", 3600 * 24 * 7, profile.build().toByteArray)
      }

    ctx.stop()
  }

  def merge(x: ProfileRow, y: ProfileRow): ProfileRow = {
    x.copy(pkgs = x.pkgs ::: y.pkgs)
  }
}

