package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.anal.MergeLog.createSuccessMarkHDFSFile
import com.cpc.spark.log.parser._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable


/**
  * /warehouse/dl_cpc.db/logparsed_cpc_search_minute
  * /warehouse/dl_cpc.db/logparsed_cpc_click_minute
  * /warehouse/dl_cpc.db/logparsed_cpc_show_minute
  * (searchid,ideaid)
  */
object MergeParsedLog {

  //数据源根目录
  var srcRoot = ""
  var prefix = ""
  var suffix = ""

  //时间格式
  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  //当前日期
  var g_date = new Date()

  def main(args: Array[String]): Unit = {
    //参数不对
    if (args.length < 3) {
      System.err.println(
        s"""
           |usage: analunionlog <hdfs_input> <hdfs_ouput> <hour_before>
         """.stripMargin
      )
      System.exit(1)
    }

    //设置日志级别
    Logger.getRootLogger.setLevel(Level.WARN)

    srcRoot = args(0)
    val mergeTbl = args(1)
    val hourBefore = args(2).toInt
    prefix = args(3) //"" 空
    suffix = args(4) //"" 空

    val cal = Calendar.getInstance()
    g_date = cal.getTime //以后只用这个时间
    cal.add(Calendar.HOUR, -hourBefore) //hourBefore前的 时间
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime) //年月日
    val hour = new SimpleDateFormat("HH").format(cal.getTime) //小时

    //获得sparksession
    val spark = SparkSession.builder()
      .appName("union log %s partition = %s".format(mergeTbl, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    /**
      * search
      */
    var searchRDD = prepareSourceString(spark, prefix + "cpc_search" + suffix, hourBefore, 1)

    // RDD为空，退出
    if (searchRDD == null) {
      System.err.println("search data is empty")
      System.exit(1)
    }

    //输出一一个元素
    //    searchRDD.take(1).foreach {
    //      x => println(x)
    //    }

    // 去重，转为PairRDD
    val searchData2 = searchRDD
      .as[UnionLog]
      .rdd
      .filter(_ != null)
      .map(x => ((x.searchid, x.ideaid), x)) //((searchid,ideaid), UnionLog)
      .reduceByKey((x, y) => x) //去重
      .map { //覆盖时间，防止记日志的时间与flume推日志的时间不一致造成的在整点出现的数据丢失，下面的以search为准
      x =>
        var ulog = x._2.copy(date = date, hour = hour)
        (x._1, ulog) //Pair RDD
    }

    /**
      * show
      */
    var showRDD = prepareSourceString(spark, prefix + "cpc_show" + suffix, hourBefore, 2)

    /**
      * 获得每个广告 '本次播放最大时长' 的日志； 转为PairRDD
      */
    var showData2 = showRDD
      .as[ShowLog]
      .rdd
      .filter(_ != null)
      .map(x => ((x.searchid,x.ideaid), Seq(x))) //((searchid,ideaid), Seq(ShowLog))
      .reduceByKey((x, y) => x ++ y)
      .map {
        x =>
          val logs = x._2
          var headLog = logs.head //获得第一个元素
          val headLog_VideoShowTime = headLog.video_show_time //获得第一个元素的video_show_time
          logs.foreach {
            y =>
              if (y.video_show_time > headLog_VideoShowTime) {
                headLog = y //获得 '本次播放最大时长' 的日志
              }
          }
          (x._1, headLog) //((searchid,ideaid), Seq(ShowLog))
      }


    /**
      * click
      */
    var clickRDD = prepareSourceString(spark, prefix + "cpc_click" + suffix, hourBefore, 2)

    var clickData2: RDD[((String, Int), ClickLog)] = null

    if (clickRDD != null) {
      clickData2 = clickRDD
        .as[ClickLog]
        .rdd
        .map(x => ((x.searchid,x.ideaid), Seq(x))) //((searchid,ideaid), Seq(ClickLog))
        .reduceByKey((x, y) => x ++ y)
        .map {
          x => //((searchid,ideaid),seq(ClickLog))
            val logs = x._2
            var clicklog = logs.head
            var notgetGood = true
            logs.foreach {
              clog =>
                if (clog.isSpamClick() == 1) {
                  //作弊点击，重复点击
                  clicklog.spam_click += 1
                } else {
                  if (notgetGood) {
                    clicklog.isclick = clog.isclick
                    clicklog.click_timestamp = clog.click_timestamp
                    clicklog.antispam_score = clog.antispam_score
                    clicklog.antispam_rules = clog.antispam_rules
                    clicklog.click_network = clog.click_network
                    clicklog.click_ip = clog.click_ip

                    clicklog.touch_x = clog.touch_x
                    clicklog.touch_y = clog.touch_y
                    clicklog.slot_width = clog.slot_width
                    clicklog.slot_height = clog.slot_height
                    clicklog.antispam_predict = clog.antispam_predict
                    clicklog.click_ua = clog.click_ua

                    notgetGood = false
                  }
                }
            }
            ((clicklog.searchid, clicklog.ideaid), clicklog)
        }

    }


    /**
      * union log =>
      * search, show, click使用(searchid,ideaid)进行left outer join
      * 并将show, click的值更新到search(是UnionLog样例类)中
      */
    val unionData1 = searchData2.leftOuterJoin(showData2).leftOuterJoin(clickData2)
      .map {
        x => //( (searchid,ideaid), ((searchlog1,showlog2),clicklog3) )
          (x._1, (x._2._1._1, x._2._1._2, x._2._2)) //( (searchid,ideaid), (searchlog1,showlog2,clicklog3) )
      }
      .map {
        x =>
          var searchlog1 = x._2._1
          val showlog2 = x._2._2.getOrElse(null)
          val clicklog3 = x._2._3.getOrElse(null)

          var ext1 = mutable.Map[String, ExtValue]() ++ searchlog1.ext

          /**
            * 对log2,log3进行处理，将值更新到log1中
            */
          //对log2进行处理, 更新log1的值

          if (showlog2 != null) {

            ext1.update("show_refer", ExtValue(string_value = showlog2.show_refer))
            ext1.update("show_ua", ExtValue(string_value = showlog2.show_ua))
            ext1.update("video_show_time", ExtValue(int_value = showlog2.video_show_time))
            ext1.update("charge_type", ExtValue(int_value = showlog2.charge_type))
            searchlog1 = searchlog1.copy(
              isshow = showlog2.isshow,
              show_timestamp = showlog2.show_timestamp,
              show_network = showlog2.show_network,
              show_ip = showlog2.show_ip,
              ext = ext1
            )
          }

          //对log3（click log）进行处理，更新log1的值
          if (clicklog3 != null) {

            val spam = clicklog3.spam_click
            ext1.update("spam_click", ExtValue(int_value = spam))
            searchlog1 = searchlog1.copy(
              ext = ext1
            )
            if (clicklog3.isSpamClick() != 1) {
              ext1.update("touch_x", ExtValue(int_value = clicklog3.touch_x))
              ext1.update("touch_y", ExtValue(int_value = clicklog3.touch_y))
              ext1.update("slot_width", ExtValue(int_value = clicklog3.slot_width))
              ext1.update("slot_height", ExtValue(int_value = clicklog3.slot_height))
              ext1.update("antispam_predict", ExtValue(float_value = clicklog3.antispam_predict))
              ext1.update("click_ua", ExtValue(string_value = clicklog3.click_ua))

              searchlog1 = searchlog1.copy(
                isclick = clicklog3.isclick,
                click_timestamp = clicklog3.click_timestamp,
                antispam_score = clicklog3.antispam_score,
                antispam_rules = clicklog3.antispam_rules,
                click_network = clicklog3.click_network,
                click_ip = clicklog3.click_ip,
                ext = ext1
              )
            }
          }
          (searchlog1.searchid, searchlog1)
      }

    val unionData = unionData1.groupByKey(1000) //设置1000个分区
      .map {
      rec => //(searchlog1.searchid, searchlog1[UnionLog])
        val logs = rec._2
        if (logs.head.adslot_type == 7) {
          var motivation = Seq[Motivation]()
          var motive_ext = Seq[Map[String, String]]()
          val head = logs.head
          for (log <- logs) {
            val m = Motivation(log.userid, log.planid, log.unitid, log.ideaid, log.bid, log.price, log.isfill,
              log.isshow, log.isclick)
            val m_ext = Map("ideaid" -> log.ideaid.toString, "downloaded_app" -> log.ext_string.getOrElse("downloaded_app", ""))
            motivation = motivation :+ m
            motive_ext = motive_ext :+ m_ext
          }
          head.copy(motivation = motivation, motive_ext = motive_ext)
        } else
          rec._2.head
    }

    spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(mergeTbl, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
      """.stripMargin.format(mergeTbl, date, hour, mergeTbl, date, hour))
    println("union done")

    //    createSuccessMarkHDFSFile(date, hour, "union_done") //创建成功标记文件


  }


  /**
    * 获得数据
    */
  def prepareSourceString(ctx: SparkSession, src: String, hourBefore: Int, hours: Int): Dataset[Row] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourPath(hourBefore, hours))
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08}/*
    val readData = ctx.read
      .parquet(input)

    readData
  }

  //获取 {yyyy-MM-dd/HH,yyyy-MM-dd/HH}
  def getDateHourPath(hourBefore: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(g_date) //当前日期
    val parts = new Array[String](hours)
    cal.add(Calendar.HOUR, -hourBefore) //前一个小时 时间
    for (h <- 0 until hours) {
      parts(h) = partitionPathFormat.format(cal.getTime) //yyyy-MM-dd/HH
      cal.add(Calendar.HOUR, 1)
    }
    "{" + parts.mkString(",") + "}"
  }
}
