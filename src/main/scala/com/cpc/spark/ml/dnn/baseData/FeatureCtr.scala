package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import org.apache.commons.lang3.time.DateFormatUtils
import java.util.Date
import java.text.DateFormat

import scala.collection.mutable
import scala.collection.mutable.Map

/**
  * 解析tfrecord到hdfs并统计反馈ctr特征
  * created time : 2019/07/31 13:56
  * @author fenghuabin
  * @version 1.0
  *
  */

object FeatureCtr {

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
    if (args.length != 9) {
      System.err.println(
        """
          |you have to input 9 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val Array(one_hot_feature_names, src_dir, des_dir, cur_date, begin_date, end_date, numPartitions, count_one_hot, count_multi_hot) = args

    Logger.getRootLogger.setLevel(Level.WARN)
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val name_list_one_hot = one_hot_feature_names.split(",")
    if (count_one_hot.toInt != name_list_one_hot.length) {
      println("mismatched, count_one_hot:%d" + count_one_hot + ", name_list_one_hot.length:" + name_list_one_hot.length.toString)
      System.exit(1)
    }

    //val src_date_list = sta_date.split(";")
    //val src_date_list = GetDataRange(begin_date, cur_date)
    val src_date_list_ori = ArrayBuffer[String]()
    val src_week_list_ori = ArrayBuffer[String]()
    val src_date_list_ori_with_week = GetDataRangeWithWeek(begin_date, end_date)
    for (pair <- src_date_list_ori_with_week) {
      src_date_list_ori += pair.split(";")(0)
      src_week_list_ori += pair.split(";")(1)
    }
    val src_date_list = src_date_list_ori.reverse
    val src_week_list = src_week_list_ori.reverse

    println("src_date_list:" + src_date_list.mkString(";"))
    println("src_week_list:" + src_week_list.mkString(";"))
    println("src_date_list_with_week:" + src_date_list_ori_with_week.mkString("|"))

    val ctrMap: mutable.Map[String, (Long, Long)] = mutable.Map()
    val ctrMapBC = sc.broadcast(ctrMap)

    /** **********make ctr statistics ************************/
    println("Make ctr statistics")
    for (date_idx <- src_date_list.indices) {
      val src_date = src_date_list(date_idx)
      val src_week = src_week_list(date_idx)
      println("date:" + src_date + ", week:" + src_week)
      val curr_file_src = src_dir + "/" + src_date
      val tf_ctr = des_dir + "/" + src_date + "-ctr"
      if (!exists_hdfs_path(tf_ctr) && exists_hdfs_path(curr_file_src)) {
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

            var dense_str: Seq[String] = null
            dense_str = dense.map(_.toString) ++ Seq[String](src_week)

            var count = ""
            if (label_arr.head == 1L) {
              count = "1,1"
            } else {
              count = "0,1"
            }

            val output = scala.collection.mutable.ArrayBuffer[String]()
            output += "week_" + src_week + ";" + count
            for (idx <- 0 until count_one_hot.toInt) {
              output += name_list_one_hot(idx) + "_" + dense(idx).toString + ";" + count
              //var curr_count = (0L, 0L)
              //if (ctrMapBC.value.contains(curr_feature)) {
              //  curr_count = ctrMapBC.value(curr_feature)
              //}
              //ctrMapBC.value += (curr_feature -> (curr_count._1 + positive, curr_count._2 + negative))
            }
            output.mkString("\t")
          }
        ).flatMap(
          rs => {
            val line_list = rs.split("\t")
            for (elem <- line_list)
              yield (elem.split(";")(0), (elem.split(";")(1).split(",")(0).toLong, elem.split(";")(1).split(",")(1).toLong))
          }
        ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).sortByKey().map({rs=>rs._1 + "\t" + rs._2._1 + "\t" + rs._2._2}).repartition(1).saveAsTextFile(tf_ctr)
      }
    }
    println("Done.......")
  }
}
