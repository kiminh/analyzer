package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.FeatureParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
样本
 */
object AdvSvm {

  var redis: RedisClient = _

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <daybefore > <days> <user click>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val ctx = SparkSession.builder()
      .appName("GenerateAdvSvm v5")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    val conf = ConfigFactory.load()
    redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis.select(5)

    println("read user info")
    FeatureParser.loadUserInfo(args(2))
    println("done", FeatureParser.userClk.size, FeatureParser.userPV.size)

    val userClk = FeatureParser.userClk
    val userPV = FeatureParser.userPV

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)

      val rawlog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" and isfill = 1 and adslotid > 0
        """.stripMargin.format(date))
        .as[UnionLog].rdd
        .filter {
          u =>
            var ret = false
            if (u != null && u.searchid.length > 0) {
              //TODO network数据不准确暂时忽略
              if (u.sex > 0 && u.coin > 0 && u.age > 0 && u.os > 0) {
                //1 / 20 负样本
                if (u.isclick == 1 || Random.nextInt(20) == 0) {
                  ret = true
                }
              }
            }
            ret
        }
        .cache()

      val svm = rawlog.toLocalIterator
        .map {
          u =>
            FeatureParser.parseUnionLog(u)
        }
        .filter(_.length > 0)

      ctx.sparkContext.parallelize(svm.toSeq)
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/v5/" + date)

      println("done", rawlog.count())
      rawlog.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}

