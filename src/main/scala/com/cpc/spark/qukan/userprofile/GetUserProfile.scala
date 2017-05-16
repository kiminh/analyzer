package com.cpc.spark.qukan.userprofile

import java.util.Calendar

import com.cpc.spark.qukan.parser.{HdfsParser, ProfileRow}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
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
    Logger.getRootLogger.setLevel(Level.WARN)
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
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    //user preferred type
    val memberDeviceId = ctx.sql(
      """
        |select member_id,device_code from gobblin.qukan_p_member_info
      """.stripMargin)
      .rdd
      .map {
        x =>
          try {
            val id = x.getString(0).toLong
            val uid = x.getString(1)
            if (id > 0 && uid.length > 0) {
              (id, (uid, 0))
            } else {
              null
            }
          } catch {
            case e: Exception => null
          }
      }
      .filter(_ != null)

    val memberPcate = ctx.sql(
      """
        |select member_id,type from algo_lechuan.user_preferred_type
      """.stripMargin)
      .rdd
      .map {
        x =>
          try {
            val id = x.getString(0).toLong
            val cate = x.getString(1).toInt
            if (id > 0 && cate > 0) {
              (id, ("", cate))
            } else {
              null
            }
          } catch {
            case e: Exception => null
          }
      }
      .filter(_ != null)

    val pcateRdd = memberDeviceId.union(memberPcate)
      .reduceByKey {
        (x, y) =>
          var uid = ""
          var cate = 0
          if (x._1.length > 0) {
            uid = x._1
          } else {
            uid = y._1
          }
          if (x._2 > 0) {
            cate = x._2
          } else {
            cate = y._2
          }
          (uid, cate)
      }
      .filter(_._2._1.length > 0)
      .map {
        x =>
          ProfileRow(
            devid = x._2._1,
            pcate = x._2._2,
            from = 2
          )
      }

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

    unionRdd.union(pcateRdd).map(x => (x.devid, x))
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
            .setPcategory(x.pcate)

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
    var cate = 0
    if (x.pcate > 0) {
      cate = x.pcate
    }
    if (y.pcate > 0) {
      cate = y.pcate
    }
    x.copy(
      pcate = cate,
      pkgs = x.pkgs ::: y.pkgs
    )
  }
}

