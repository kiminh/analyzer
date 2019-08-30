package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

/**
  * 解析tfrecord并统计广告相关id
  * created time : 2019/07/13 10:38
  * @author fenghuabin
  * @version 1.0
  *
  */

object MakeUserIdStatistics {



  def delete_hdfs_path(path: String): Unit = {

    val conf = new org.apache.hadoop.conf.Configuration()
    val p = new org.apache.hadoop.fs.Path(path)
    val hdfs = p.getFileSystem(conf)
    val hdfs_path = new org.apache.hadoop.fs.Path(path.toString)

    //val hdfs_path = new org.apache.hadoop.fs.Path(path.toString)
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(hdfs_path)) {
      hdfs.delete(hdfs_path, true)
    }
  }

  def exists_hdfs_path(path: String): Boolean = {

    val conf = new org.apache.hadoop.conf.Configuration()
    val p = new org.apache.hadoop.fs.Path(path)
    val hdfs = p.getFileSystem(conf)
    val hdfs_path = new org.apache.hadoop.fs.Path(path.toString)
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())

    if (hdfs.exists(hdfs_path)) {
      true
    } else {
      false
    }
  }

  def writeNum2File(file: String, num: Long): Unit = {
    val writer = new PrintWriter(new File(file))
    writer.write(num.toString)
    writer.close()
  }

  def formatDate(date: Date, pattern: String="yyyy-MM-dd"): String = {
    val formatDate = DateFormatUtils.format(date, pattern)
    formatDate
  }

  def GetDataRangeWithWeek(beginStr: String, endStr: String, format : String = "yyyy-MM-dd"): ArrayBuffer[String] = {
    val ranges = ArrayBuffer[String]()
    val sdf = new SimpleDateFormat(format)
    var dateBegin = sdf.parse(beginStr)
    val dateEnd = sdf.parse(endStr)
    while (dateBegin.compareTo(dateEnd) <= 0) {
      ranges += sdf.format(dateBegin) + ";" + DateFormatUtils.format(dateBegin, "E")
      dateBegin = DateUtils.addDays(dateBegin, 1)
    }
    ranges
  }

  def GetDataRange(beginStr: String, endStr: String, format : String = "yyyy-MM-dd"): ArrayBuffer[String] = {
    val ranges = ArrayBuffer[String]()
    val sdf = new SimpleDateFormat(format)
    var dateBegin = sdf.parse(beginStr)
    val dateEnd = sdf.parse(endStr)
    while (dateBegin.compareTo(dateEnd) <= 0) {
      ranges += sdf.format(dateBegin)
      dateBegin = DateUtils.addDays(dateBegin, 1)
    }
    ranges
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println(
        """
          |you have to input 5 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(src_dir, date_begin, date_end, des_dir, today_date) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //val src_date_list = src_date_str.split(";")
    val src_date_list = ArrayBuffer[String]()
    val src_date_list_base = ArrayBuffer[String]()
    val src_date_list_with_week = GetDataRangeWithWeek(date_begin, date_end)
    for (pair <- src_date_list_with_week) {
      src_date_list += pair.split(";")(0)
      src_date_list_base += pair.split(";")(0)
    }
    println("src_date_list:" + src_date_list.mkString(";"))

    /** **********make text examples ************************/
    println("Make text examples")
    src_date_list_base += today_date
    for (date_idx <- src_date_list_base.indices) {
      val src_date = src_date_list_base(date_idx)
      val curr_file_src = src_dir + "/" + src_date + "/_SUCCESS"
      val tf_text = des_dir + "/" + src_date + "-text"
      if (!exists_hdfs_path(tf_text) && exists_hdfs_path(curr_file_src)) {
        val curr_file_src_collect = src_dir + "/" + src_date + "/part-r-*"
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src_collect)

        importedDf.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx0 = rs.getSeq[Long](3)
            val sample_idx = rs.getLong(4)
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            //11,12,13,14
            //,ideaid,unitid,planid,userid
            (dense(11).toString, dense(12).toString, dense(13).toString, dense(14).toString)

            val output = scala.collection.mutable.ArrayBuffer[String]()
            output += dense(11).toString
            output += dense(12).toString
            output += dense(13).toString
            output += dense(14).toString
            output.mkString("\t")
          }
        ).repartition(10).saveAsTextFile(tf_text)
      }
    }
    println("Done.......")

    /** **********make text examples ************************/
    println("calc existing ids")


    //var array = ArrayBuffer[String]()
    //val cntStaMap = scala.collection.mutable.HashMap.empty[Int, Int]
    //if (cntStaMap.contains(pvMapCnt(i))) {
    //  cntStaMap(pvMapCnt(i)) = cntStaMap(pvMapCnt(i)) + 1
    //} else {
    //  cntStaMap(pvMapCnt(i)) = 1
    //}

    //,ideaid,unitid,planid,userid
    val idealIdMap = scala.collection.mutable.HashMap.empty[String, Int]
    val unitIdMap = scala.collection.mutable.HashMap.empty[String, Int]
    val planIdMap = scala.collection.mutable.HashMap.empty[String, Int]
    val userIdMap = scala.collection.mutable.HashMap.empty[String, Int]

    val idealIdMapToday = scala.collection.mutable.HashMap.empty[String, Int]
    val unitIdMapToday = scala.collection.mutable.HashMap.empty[String, Int]
    val planIdMapToday = scala.collection.mutable.HashMap.empty[String, Int]
    val userIdMapToday = scala.collection.mutable.HashMap.empty[String, Int]

    val idealIdMapBC = sc.broadcast(idealIdMap)
    val unitIdMapBC = sc.broadcast(unitIdMap)
    val planIdMapBC = sc.broadcast(planIdMap)
    val userIdMapBC = sc.broadcast(userIdMap)

    val idealIdMapTodayBC = sc.broadcast(idealIdMapToday)
    val unitIdMapTodayBC = sc.broadcast(unitIdMapToday)
    val planIdMapTodayBC = sc.broadcast(planIdMapToday)
    val userIdMapTodayBC = sc.broadcast(userIdMapToday)

    val staInfo = scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.HashMap[String, Int]]
    val staInfoBC = sc.broadcast(staInfo)

    var data = sc.parallelize(Array[(String, Long)]())

    for (date_idx <- src_date_list.indices) {
      val src_date = src_date_list(date_idx)
      val tf_text = des_dir + "/" + src_date + "-text"
      val user_ideal_file = des_dir + "/" + src_date + "-user-ideal"
      if (exists_hdfs_path(tf_text)) {
        println("now " + tf_text)
        val data_tmp = sc.textFile(tf_text).map(
          rs => {
            val line_list = rs.split("\t")

            val idealId = line_list(0)
            val unitId = line_list(1)
            val planId = line_list(2)
            val userId = line_list(3)

            if (idealIdMapBC.value.contains(idealId)) {
              idealIdMapBC.value(idealId) = idealIdMapBC.value(idealId) + 1
            } else {
              idealIdMapBC.value(idealId) = 1
            }

            if (unitIdMapBC.value.contains(unitId)) {
              unitIdMapBC.value(unitId) = unitIdMapBC.value(unitId) + 1
            } else {
              unitIdMapBC.value(unitId) = 1
            }

            if (planIdMapBC.value.contains(planId)) {
              planIdMapBC.value(planId) = planIdMapBC.value(planId) + 1
            } else {
              planIdMapBC.value(planId) = 1
            }

            if (userIdMapBC.value.contains(userId)) {
              userIdMapBC.value(userId) = userIdMapBC.value(userId) + 1
            } else {
              userIdMapBC.value(userId) = 1
            }
            (userId + "_" + idealId, 1L)
          }
        ).reduceByKey(_ + _).repartition(1).saveAsTextFile(user_ideal_file)
      }
    }
    println("Done.......")

    //val user_ideal_info = des_dir + "/" + "user-ideal-info"
    //data.reduceByKey(_ + _).repartition(1).sortBy(_._2 * -1).map {
    //  case (key, value) =>
    //    key + "\t" + value.toString
    //}.saveAsTextFile(user_ideal_info)


    println("idealIdMap Size:" + idealIdMapBC.value.size)
    println("unitIdMap Size:" + unitIdMapBC.value.size)
    println("planIdMap Size:" + planIdMapBC.value.size)
    println("userIdMap Size:" + userIdMapBC.value.size)


    val tf_text = des_dir + "/" + today_date + "-text"
    if (exists_hdfs_path(tf_text)) {
      sc.textFile(tf_text).map(
        rs => {
          val line_list = rs.split("\t")

          val idealId = line_list(0)
          val unitId = line_list(1)
          val planId = line_list(2)
          val userId = line_list(3)

          if (idealIdMapTodayBC.value.contains(idealId)) {
            idealIdMapTodayBC.value(idealId) = idealIdMapTodayBC.value(idealId) + 1
          } else {
            idealIdMapTodayBC.value(idealId) = 1
          }

          if (unitIdMapTodayBC.value.contains(unitId)) {
            unitIdMapTodayBC.value(unitId) = unitIdMapTodayBC.value(unitId) + 1
          } else {
            unitIdMapTodayBC.value(unitId) = 1
          }

          if (planIdMapTodayBC.value.contains(planId)) {
            planIdMapTodayBC.value(planId) = planIdMapTodayBC.value(planId) + 1
          } else {
            planIdMapTodayBC.value(planId) = 1
          }

          if (userIdMapTodayBC.value.contains(userId)) {
            userIdMapTodayBC.value(userId) = userIdMapTodayBC.value(userId) + 1
          } else {
            userIdMapTodayBC.value(userId) = 1
          }
        }
      )
    }

    println("idealIdMapToday Size:" + idealIdMapTodayBC.value.size)
    println("unitIdMapToday Size:" + unitIdMapTodayBC.value.size)
    println("planIdMapToday Size:" + planIdMapTodayBC.value.size)
    println("userIdMapToday Size:" + userIdMapTodayBC.value.size)

    var idealIdOldCnt = 0
    var unitIdOldCnt = 0
    var planIdOldCnt = 0
    var userIdOldCnt = 0

    idealIdMapTodayBC.value.foreach{
      case (e,i) =>
        if (idealIdMapBC.value.contains(e)) {
          idealIdOldCnt += 1
        }
    }

    unitIdMapTodayBC.value.foreach{
      case (e,i) =>
        if (unitIdMapBC.value.contains(e)) {
          unitIdOldCnt += 1
        }
    }

    planIdMapTodayBC.value.foreach{
      case (e,i) =>
        if (planIdMapBC.value.contains(e)) {
          planIdOldCnt += 1
        }
    }

    userIdMapTodayBC.value.foreach{
      case (e,i) =>
        if (userIdMapBC.value.contains(e)) {
          userIdOldCnt += 1
        }
    }

    println("idealIdMapToday new rate:" + idealIdOldCnt / idealIdMapBC.value.size)
    println("unitIdMapToday new rate:" + unitIdOldCnt / unitIdMapBC.value.size)
    println("planIdMapToday new rate:" + planIdOldCnt / planIdMapBC.value.size)
    println("userIdMapToday new rate:" + userIdOldCnt / userIdMapBC.value.size)


  }
}

