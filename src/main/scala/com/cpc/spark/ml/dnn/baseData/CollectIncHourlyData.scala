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
  * 解析adlistv4tfrecord特征
  * created time : 2019/09/19 23:31
  * @author fenghuabin
  * @version 1.0
  *
  */

object CollectIncHourlyData {


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

  def formatDate(date: Date, pattern: String = "yyyy-MM-dd"): String = {
    val formatDate = DateFormatUtils.format(date, pattern)
    formatDate
  }

  def GetDataRangeWithWeek(beginStr: String, endStr: String, format: String = "yyyy-MM-dd"): ArrayBuffer[String] = {
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

  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      System.err.println(
        """
          |you have to input 7 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(collect_path, des_dir, train_files_collect_1, last_date, curr_date, time_id, delete_old) = args

    println(args(0))
    println(args(1))
    println(args(2))
    println(args(3))
    println(args(4))
    println(args(5))
    println(args(6))

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext



    val curr_base_instances_rdd =
    sc.textFile(collect_path + "/hourly_curr_map_instances.data").map({
      rs =>
        val line_list = rs.split("\t")
        (line_list(0), line_list(1).toLong)
    })

    val instances_1 = collect_path + "/" + curr_date + "-" + time_id + "-incr-instances"
    if (delete_old == "true") {
      delete_hdfs_path(instances_1)
    }

    val df_train_files_collect_1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_1)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_1.printSchema()
    df_train_files_collect_1.show(3)

    val rdd_1 = df_train_files_collect_1.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)
        Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
      })

    df_train_files_collect_1.rdd.map(
      rs => {
        val idx_arr = rs.getSeq[Long](2)
        val dense = rs.getSeq[Long](6)

        val output: Array[String] = new Array[String](dense.length + idx_arr.length)
        for (idx <- dense.indices) {
          output(idx) = dense(idx).toString
        }
        for (idx <- idx_arr.indices) {
          output(idx + dense.length) = idx_arr(idx).toString
        }
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).leftOuterJoin(curr_base_instances_rdd).map({
      rs =>
        val key = rs._1
        val value_left = rs._2._1
        val value_right = rs._2._2
        if (value_right.isEmpty) {
          (key, value_left, true)
        } else {
          (key, value_left, false)
        }
    }).filter(rs => rs._3).
      map({ rs => rs._1 + "\t" + rs._2 }).
      repartition(1).
      saveAsTextFile(instances_1)
  }
}

