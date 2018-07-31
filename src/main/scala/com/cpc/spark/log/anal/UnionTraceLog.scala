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
  val partitionPathFormat1 = new SimpleDateFormat("yyyy-MM-dd/HH")
  val partitionPathFormat_unionlog = new SimpleDateFormat("yyyy-MM-dd/HH")


  def main(args: Array[String]): Unit = {
    //参数小于3个
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
    val date = args(1)
    val hour = args(2)
    val minute = args(3)
    prefix = args(4) //"" 空
    suffix = args(5) //"" 空
    val unionTbl = args(6) //union_trace_table
    val unionTraceTbl = args(7) //union_trace_table

    val datee = date.split("-")
    val cal = Calendar.getInstance()
    cal.set(Calendar.YEAR, datee(0).toInt)
    cal.set(Calendar.MONTH, datee(1).toInt - 1)
    cal.set(Calendar.DAY_OF_MONTH, datee(2).toInt)
    cal.set(Calendar.HOUR_OF_DAY, hour.toInt)
    cal.set(Calendar.SECOND, 0)


    //获得sparksession
    val spark = SparkSession.builder()
      .appName("union trace log %s partition = %s".format(unionTraceTbl, partitionPathFormat1.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //读取前一个小时的Unionlog
    //val unionData = prepareSourceString2(spark, unionTbl, date, hour)
    val unionData = spark.sql(
      s"""
         |select searchid,timestamp
         |from dl_cpc.$unionTbl
         |where date='$date' and hour='$hour' and isclick > 0
      """.stripMargin)
      .map { r => (r.getAs[String](0), r.getAs[Int](1)) }
      .rdd
    

    //读取1h40min的tracelog(前一个小时和当前前40min的tracelog)
    val traceRDD = prepareSourceString(spark, prefix + "cpc_trace" + suffix, date, hour.toInt, minute.toInt, 12)

    //过滤isclick为1的unionlog,并转为pairrdd, (searchid, timestamp)
    if (traceRDD != null) {
      //      val click = unionData
      //        .as[UnionLog]
      //        .rdd
      //        .filter(_.isclick > 0)
      //        .map(x => (x.searchid, x.timestamp))

      //tracelog join unionlog
      val traceData = traceRDD
        .as[TraceLog]
        .rdd
        .map(x => (x.searchid, x))
        .filter(x => x._1 != "none" && x._1 != "")
        .join(unionData)
        .map {
          x =>
            x._2._1.copy(search_timestamp = x._2._2, date = date, hour = hour)
        }

      //数据写入hive表
      spark.createDataFrame(traceData)
        .write
        .mode(SaveMode.Overwrite)
        .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(unionTraceTbl, date, hour))

      println("~~~~~~write trace_union_data to hive successfully")

      spark.sql(
        """
          |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
          | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
        """.stripMargin.format(unionTraceTbl, date, hour, unionTraceTbl, date, hour))

      if (unionData.take(1).length > 0) {
        println("~~~~~~union trace done")
        createMarkFile(spark, "union_trace_done", date, hour)
      } else {
        println("~~~~~~union trace log failed...")
      }

    }

    createMarkFile(spark, "merge_done", date, hour)


  }


  /**
    * 根据分区获得数据
    */
  def prepareSourceString(ctx: SparkSession, src: String, date: String, hour: Int, minute: Int, minutes: Int): Dataset[Row] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourMinutePath(date, hour, minute, minutes))
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08/00}
    val readData = ctx.read
      .parquet(input)

    readData
  }

  def prepareSourceString2(ctx: SparkSession, src: String, date: String, hour: String): Dataset[Row] = {
    val input = "%s/%s/date=%s/hour=%s/*".format(srcRoot, src, date, hour)
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08/00}
    val readData = ctx.read
      .parquet(input)

    readData
  }


  /**
    * 获得数据分区
    *
    * @param date
    * @param hour
    * @param minute
    * @param minutes
    * @return
    */
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


  def getDateHourPath(date: String, hour: String): String = {
    val cal = Calendar.getInstance()

    cal.set(Calendar.YEAR, date.split("-")(0).toInt)
    cal.set(Calendar.MONTH, date.split("-")(1).toInt - 1)
    cal.set(Calendar.DAY_OF_MONTH, date.split("-")(2).toInt)
    cal.set(Calendar.HOUR_OF_DAY, hour.toInt) //前一个小时 时间

    val parts = partitionPathFormat_unionlog.format(cal.getTime) //yyyy-MM-dd/HH

    "{" + parts + "}"
  }

  def createMarkFile(spark: SparkSession, markTable: String, date: String, hour: String): Unit = {
    // 创建空rdd, 用于创建空文件
    val empty = Seq("")
    val emptyRdd = spark.sparkContext.parallelize(empty)

    import spark.implicits._
    emptyRdd.toDF
      .write
      .mode(SaveMode.Overwrite)
      .text("/warehouse/cpc/%s/%s-%s.ok".format(markTable, date, hour))
  }

}
