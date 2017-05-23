package com.cpc.spark.log.anal

import java.util.Calendar

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/**
  * Created by Roy on 2017/5/18.
  *
  */
object AnalTouchedUV {

  /*统计维度
  地域
  性别   暂时通过评价随机
  年龄   暂时通过平均随机
  人群分类
  操作系统
  网络环境
  投放时间
   */
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
    val date = LogParser.dateFormat.format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))

    val ctx = SparkSession.builder()
      .appName("anal ad touched query amount[%s]".format(date))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val log = ctx.sql("select * from dl_cpc.cpc_union_log where `date` = \"%s\"".format(date)).as[UnionLog]
    val ret = log.rdd
      .map {
        x =>
          val rndSex = Random.nextInt(2) + 1  //随机性别
          val rndAge = Random.nextInt(6) + 1 //随机年龄
          var lvl = 0
          if (x.coin < 10) {
            lvl = 1
          } else if (x.coin < 1000) {
            lvl = 2
          } else if (x.coin < 10000) {
            lvl = 3
          } else {
            lvl = 4
          }
          AnalCond(
            province = x.province,
            sex = rndSex,
            age = rndAge,
            coin_level = lvl,
            os = x.os,
            network = x.network,
            sum = 1,
            uid = x.uid,
            date = date
          )
      }
      .cache()

    val uv = ret.map(x => (x.uid, x)).reduceByKey((x, y) => x).count()
    val ret1 = ret.map(x => (x.keyuid, x))
      .reduceByKey((x, y) => x)
      .map(x => (x._2.key, x._2))
      .reduceByKey((x, y) => x.sum(y))
      .flatMap(x => Seq(x._2, x._2.copy(sex = 0, sum = 0)))
      .flatMap(x => Seq(x, x.copy(age = 0, sum = 0)))
      .flatMap {
        x =>
          if (x.os > 0) {
            Seq(x, x.copy(os = 0, sum = 0))
          } else {
            Seq(x)
          }
      }
      .flatMap {
        x =>
          if (x.network > 0) {
            Seq(x, x.copy(network = 0, sum = 0))
          } else {
            Seq(x.copy(sum = 0))
          }
      }
      .flatMap {
        x =>
          if (x.province > 0) {
            Seq(x, x.copy(province = 0, sum = 0))
          } else {
            Seq(x.copy(sum = 0))
          }
      }
      .flatMap {
        x =>
          if (x.coin_level > 0) {
            Seq(x, x.copy(coin_level = 0, sum = 0))
          } else {
            Seq(x.copy(sum = 0))
          }
      }
      .map(x => (x.key, x))
      .reduceByKey((x, y) => x)
      .map(_._2)
      .union(ctx.sparkContext.parallelize(Seq(AnalCond(date = date, sum = uv.toInt))))
      .cache()

    ret1.toDF()
      .write
      .mode(SaveMode.Append)
      .partitionBy("date")
      .saveAsTable("dl_cpc.ad_touched_uv")

    ret1.toLocalIterator
      .foreach {
        x =>
          /*
          province-sex-age-coin_level-os-network_TOUCHEDUV
          16-1-5-0-1-1_TOUCHEDUV  => 14674
           */
          redis.set(x.key + "_TOUCHEDUV", x.sum * 2)
      }
    println(uv, ret.count(), ret1.count())
    ctx.stop()
  }
}

case class AnalCond(
                   province: Int = 0,

                   //暂时按照100分比例来算
                   sex: Int = 0,
                   age: Int = 0,

                   //coin_level 注意保证和bs一致
                   //用户积分级别. 0默认全选 1第一档用户，积分在0-10分。 2第二档用户，积分在0-1000分。 3第三档用户，积分在0-10000分。4全选
                   coin_level: Int = 0,
                   os: Int = 0,
                   network: Int = 0,
                   sum: Int = 0,
                   uid: String = "",
                   date: String = ""

               ) {


  val key = "%d-%d-%d-%d-%d-%d".format(province, sex, age, coin_level, os, network)

  val keyuid = "%d-%d-%d-%d-%d-%d-%s".format(province, sex, age, coin_level, os, network, uid)

  def sum(k: AnalCond): AnalCond = {
    copy(sum = sum + k.sum)
  }
}



