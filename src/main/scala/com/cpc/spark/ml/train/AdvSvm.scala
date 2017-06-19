package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.{FeatureParser, FeatureParserV2, UserClick}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
样本
 */
object AdvSvm extends UserClick {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <version:string> <daybefore:int> <days:int> <rate:int>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val rate = args(3).toInt
    val ctx = SparkSession.builder()
      .appName("GenerateAdvSvm " + version)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    println("read user info")
    loadUserClickFromFile()

    val u: UnionLog = null
    val clickSum = (u, 0, 0, 0, 0, 0)
    val ucRdd = ctx.sparkContext.parallelize(userClk.toSeq)
      .map(x => (x._1, (Seq(clickSum), x._2, 0))).cache()
    val upRdd = ctx.sparkContext.parallelize(userPV.toSeq)
      .map(x => (x._1, (Seq(clickSum), 0, x._2))).cache()
    val uacRdd = ctx.sparkContext.parallelize(userAdClick.toSeq)
      .map(x => (x._1, (Seq(clickSum), x._2))).cache()
    val uscRdd = ctx.sparkContext.parallelize(userSlotClick.toSeq)
      .map(x => (x._1, (Seq(clickSum), x._2))).cache()
    val usacRdd = ctx.sparkContext.parallelize(userSlotAdClick.toSeq)
      .map(x => (x._1, (Seq(clickSum), x._2))).cache()

    println("done", userClk.size, userPV.size, userAdClick.size, userSlotClick.size, userSlotAdClick.size)

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
              //rate = 0表示不做过滤取全样本
              if (rate == 0) {
                ret = true
              } else if (u.media_appsid == "80000001" || u.media_appsid == "80000002") {
                //1 / 20 负样本
                if (u.isclick == 1 || Random.nextInt(rate) == 0) {
                  ret = true
                }
              }
            }
            ret
        }
        .cache()

      println("merge click")
      val clickRdd = rawlog.map(u => (u.uid, (Seq((u, 0, 0, 0, 0, 0)), 0, 0)))
        //click pv
        .union(ucRdd)
        .union(upRdd)
        .reduceByKey((x, y) => (x._1 ++ y._1, x._2 + y._2, x._3 + y._3))
        .flatMap {
          x =>
            val v = x._2
            v._1.filter(_._1 != null).map {
              u =>
                (u._1, v._2, v._3)
            }
        }
        //ad click
        .map(x => ("%s-%d".format(x._1.uid, x._1.ideaid), (Seq((x._1, x._2, x._3, 0, 0, 0)), 0)))
        .union(uacRdd)
        .reduceByKey((x, y) => (x._1 ++ y._1, x._2 + y._2))
        .flatMap {
          x =>
            val v = x._2
            v._1.filter(_._1 != null).map {
              u =>
                (u._1, u._2, u._3, v._2)
            }
        }
        //slot click
        .map(x => ("%s-%s".format(x._1.uid, x._1.adslotid), (Seq((x._1, x._2, x._3, x._4, 0, 0)), 0)))
        .union(uscRdd)
        .reduceByKey((x, y) => (x._1 ++ y._1, x._2 + y._2))
        .flatMap {
          x =>
            val v = x._2
            v._1.filter(_._1 != null).map {
              u =>
                (u._1, u._2, u._3, u._4, v._2)
            }
        }
        //slot ad click
        .map(x => ("%s-%s-%d".format(x._1.uid, x._1.adslotid, x._1.ideaid), (Seq((x._1, x._2, x._3, x._4, x._5, 0)), 0)))
        .union(usacRdd)
        .reduceByKey((x, y) => (x._1 ++ y._1, x._2 + y._2))
        .flatMap {
          x =>
            val v = x._2
            v._1.filter(_._1 != null).map {
              u =>
                (u._1, u._2, u._3, u._4, u._5, v._2)
            }
        }
        .cache()

      if (n == 1) {
        clickRdd.filter(_._6 > 0).take(20).foreach(x => println(x._1.uid, x._2, x._3, x._4, x._5, x._6))
      }
      println("done")

      clickRdd
        .map{
          x =>
            FeatureParser.parseUnionLog(x._1, x._2, x._3, x._4, x._5, x._6)
        }
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/" + version + "/" + date)

      println("done", rawlog.count())
      rawlog.unpersist()
      clickRdd.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}

