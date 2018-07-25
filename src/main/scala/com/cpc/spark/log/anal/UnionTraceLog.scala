package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * cpc_union_trace_log从MergeParsedLog2中分离出来
  */
object UnionTraceLog {
  //数据源根目录
  var srcRoot = ""
  var prefix = ""
  var suffix = ""

  //时间格式
  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH/mm")
  val partitionPathFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


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
    //    val hourBefore = args(2).toInt
    val date = args(2)
    val hour = args(3)
    val minute = args(4)
    prefix = args(5) //"" 空
    suffix = args(6) //"" 空
    val unionTraceTbl = args(7) //union_trace_table
    //val addData = args(8)  //标记补充数据

    val datee = date.split("-")
    val cal = Calendar.getInstance()
    cal.set(Calendar.YEAR, datee(0).toInt)
    cal.set(Calendar.MONTH, datee(1).toInt - 1)
    cal.set(Calendar.DAY_OF_MONTH, datee(2).toInt)
    cal.set(Calendar.HOUR_OF_DAY, hour.toInt)
    cal.set(Calendar.MINUTE, minute.toInt)
    cal.set(Calendar.SECOND, 0)


    //获得sparksession
    val spark = SparkSession.builder()
      .appName("union log %s partition = %s".format(mergeTbl, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val unionData = prepareSourceString2(spark, "cpc_union_parsedlog", date, hour.toInt, 0)

    val traceRDD = prepareSourceString(spark, prefix + "cpc_trace" + suffix, date, hour.toInt, minute.toInt, 6)

    if (traceRDD != null) {
      val click = unionData
        .as[UnionLog]
        .rdd
        .filter(
          r => {
            val millis = cal.getTimeInMillis
            val endmillis = millis + 1800000
            r.timestamp*1000 >= millis && r.timestamp*1000 < endmillis
          }
        ).filter(_.isclick > 0)
        .map(x => (x.searchid, x.timestamp))

      val traceData = traceRDD
        .as[TraceLog]
        .rdd
        .map(x => (x.searchid, x))
        .filter(_._1 != "none")
        .join(click)
        .map {
          x =>
            x._2._1.copy(search_timestamp = x._2._2, date = date, hour = hour)
        }

      spark.createDataFrame(traceData)
        .write
        .mode(SaveMode.Append)
        .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(unionTraceTbl, date, hour))

      println("write trace_union_data to hive successfully")

      spark.sql(
        """
          |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
          | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
        """.stripMargin.format(unionTraceTbl, date, hour, unionTraceTbl, date, hour))
    }


  }


  /**
    * 获得数据
    */
  def prepareSourceString(ctx: SparkSession, src: String, date: String, hour: Int, minute: Int, minutes: Int): Dataset[Row] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourMinutePath(date, hour, minute, minutes))
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08/00}
    val readData = ctx.read
      .parquet(input)

    readData
  }

  def prepareSourceString2(ctx: SparkSession, src: String, date: String, hour: Int, hours: Int): Dataset[Row] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourPath(date, hour, hours))
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08/00}
    val readData = ctx.read
      .parquet(input)

    readData
  }


  def getDateHourMinutePath(date: String, hour: Int, minute: Int, minutes: Int): String = {
    val cal = Calendar.getInstance()
    val parts = new Array[String](minutes)
    cal.set(Calendar.YEAR, date.split("-")(0).toInt)
    cal.set(Calendar.MONTH, date.split("-")(1).toInt - 1)
    cal.set(Calendar.DAY_OF_MONTH, date.split("-")(2).toInt)
    cal.set(Calendar.HOUR_OF_DAY, hour) //前一个小时 时间
    cal.set(Calendar.MINUTE, minute)
    cal.set(Calendar.SECOND, 0)

    for (h <- 0 until minutes) {
      parts(h) = partitionPathFormat.format(cal.getTime) //yyyy-MM-dd/HH/mm
      cal.add(Calendar.MINUTE, 10)
    }
    "{" + parts.mkString(",") + "}"
  }


  def getDateHourPath(date: String, hour: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    val partitionPathFormat_unionlog = new SimpleDateFormat("yyyy-MM-dd/HH")

    cal.set(Calendar.YEAR, date.split("-")(0).toInt)
    cal.set(Calendar.MONTH, date.split("-")(1).toInt - 1)
    cal.set(Calendar.DAY_OF_MONTH, date.split("-")(2).toInt)
    cal.set(Calendar.HOUR_OF_DAY, hour) //前一个小时 时间

    val parts = partitionPathFormat_unionlog.format(cal.getTime) //yyyy-MM-dd/HH

    "{" + parts + "}"
  }

}
