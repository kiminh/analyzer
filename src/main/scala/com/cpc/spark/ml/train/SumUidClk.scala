package com.cpc.spark.ml.train

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 06/06/2017.
  */
object SumUidClk {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <hive_table> <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val ctx = SparkSession.builder()
      .appName("sum uid clk")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("touched_uv.redis.host"), conf.getInt("touched_uv.redis.port"))
    redis.select(5)

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
            if (u != null && u.searchid.length > 0 && u.uid.length > 0) {
              //TODO network数据不准确暂时忽略
              if (u.sex > 0 && u.coin > 0 && u.age > 0 && u.os > 0) {
                ret = true
              }
            }
            ret
        }
        .map(u => (u.uid, (u.isclick, 1)))
        .reduceByKey {
          (x, y) =>
            (x._1 + y._1, x._2 + y._2)
        }
        .filter(_._2._1 > 0)
        .cache()

      val w = new PrintWriter("./uidclk_%s.txt".format(date))
      var c = 0
      rawlog.toLocalIterator
        .foreach {
          x =>
            val sum = x._2
            c += 1
            w.write("%s\t%d\t%d\n".format(x._1, sum._1, sum._2))
        }

      w.close()
      rawlog.unpersist()
      cal.add(Calendar.DATE, 1)
      println("done", date, c)
    }
    ctx.stop()
  }
}
