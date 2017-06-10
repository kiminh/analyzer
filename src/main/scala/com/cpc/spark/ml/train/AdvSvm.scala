package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.{FeatureParser, UserClickPV}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
样本
 */
object AdvSvm extends UserClickPV {

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
    loadUserInfo(args(2))
    val clkRdd = ctx.sparkContext.parallelize(userClk.toSeq, 13)
    val pvRdd = ctx.sparkContext.parallelize(userPV.toSeq, 13)
    val clkpv = clkRdd.map(x => (x._1, (x._2, 0, Seq[UnionLog]())))
      .union(pvRdd.map(x => (x._1, (0, x._2, Seq[UnionLog]()))))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, Seq[UnionLog]()))
      .cache()
    println("done", userClk.size, userPV.size)

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

      clkpv.union(rawlog.filter(_.uid.length > 0).map(u => (u.uid, (0, 0, Seq(u)))))
        .reduceByKey {
          (x, y) =>
            (x._1 + y._1, x._2 + y._2, x._3 ++ y._3)
        }
        .flatMap {
          x =>
            val clk = x._2._1
            val pv = x._2._2
            x._2._3.map {
              u =>
                FeatureParser.parseUnionLog(u, clk, pv)
            }
        }
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

