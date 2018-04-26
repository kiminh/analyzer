package com.cpc.spark.ml.train

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
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
        .filter(u => u != null && u.searchid.length > 0 && u.uid.length > 5)
        .cache()

      //uid click
      val w = new PrintWriter("/home/cpc/t/user_click/uidclk_%s.txt".format(date))
      var c = 0
      rawlog.map(u => (u.uid, (u.isclick, 1)))
        .reduceByKey {
          (x, y) =>
            (x._1 + y._1, x._2 + y._2)
        }
        .filter(_._2._1 > 0)
        .toLocalIterator
        .foreach {
          x =>
            val sum = x._2
            c += 1
            w.write("%s\t%d\t%d\n".format(x._1, sum._1, sum._2))
        }

      w.close()
      println("user click", c)

      //uid ad click
      val w1 = new PrintWriter("/home/cpc/t/user_click/uid_ad_clk_%s.txt".format(date))
      var c1 = 0
      rawlog.map(u => ((u.uid, u.ideaid), (u.isclick, 1)))
        .reduceByKey {
          (x, y) =>
            (x._1 + y._1, x._2 + y._2)
        }
        .filter(_._2._1 > 0)
        .toLocalIterator
        .foreach {
          x =>
            val u = x._1
            val sum = x._2
            c1 += 1
            w1.write("%s\t%d\t%d\t%d\n".format(u._1, u._2, sum._1, sum._2))
        }

      w1.close()
      println("user ad click", c1)

      //uid adslot click
      val w2 = new PrintWriter("/home/cpc/t/user_click/uid_slot_clk_%s.txt".format(date))
      var c2 = 0
      rawlog.map(u => ((u.uid, u.adslotid), (u.isclick, 1)))
        .reduceByKey {
          (x, y) =>
            (x._1 + y._1, x._2 + y._2)
        }
        .filter(_._2._1 > 0)
        .toLocalIterator
        .foreach {
          x =>
            val sum = x._2
            c2 += 1
            w2.write("%s\t%s\t%d\t%d\n".format(x._1._1, x._1._2, sum._1, sum._2))
        }
      w2.close()
      println("user slot click", c2)

      //uid adslot ad click
      val w3 = new PrintWriter("/home/cpc/t/user_click/uid_slot_ad_clk_%s.txt".format(date))
      var c3 = 0
      rawlog.map(u => ((u.uid, u.adslotid, u.ideaid), (u.isclick, 1)))
        .reduceByKey {
          (x, y) =>
            (x._1 + y._1, x._2 + y._2)
        }
        .filter(_._2._1 > 0)
        .toLocalIterator
        .foreach {
          x =>
            val u = x._1
            val sum = x._2
            c3 += 1
            w3.write("%s\t%s\t%d\t%d\t%d\n".format(u._1, u._2, u._3, sum._1, sum._2))
        }
      w3.close()
      println("user slot ad click", c3)

      rawlog.unpersist()
      cal.add(Calendar.DATE, 1)
      println("done", date)
    }
    ctx.stop()
  }
}
