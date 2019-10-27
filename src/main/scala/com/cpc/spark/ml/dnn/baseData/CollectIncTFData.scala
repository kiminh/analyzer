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

object CollectIncTFData{


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
    if (args.length != 10) {
      System.err.println(
        """
          |you have to input 10 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(des_dir, train_files_collect_0, train_files_collect_8, train_files_collect_4, train_files_collect_2, train_files_collect_1, test_file, curr_date, time_id, delete_old) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext



    val base_instances_file = des_dir + "/" + curr_date + "-base-instances-all"
    if (!exists_hdfs_path(base_instances_file + "/_SUCCESS")) {
      println("no base instances file:" + base_instances_file + ", existing...")
      return
    }

    val weighted_file_collect_0 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-0"
    val weighted_file_collect_8 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-8"
    val weighted_file_collect_4 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-4"
    val weighted_file_collect_2 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-2"
    val weighted_file_collect_1 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-1"
    val ctr_file = des_dir + "/" + curr_date + "-" + time_id + "-ctr"

    if (delete_old == "true") {
      delete_hdfs_path(weighted_file_collect_0)
      delete_hdfs_path(weighted_file_collect_8)
      delete_hdfs_path(weighted_file_collect_4)
      delete_hdfs_path(weighted_file_collect_2)
      delete_hdfs_path(weighted_file_collect_1)
      delete_hdfs_path(ctr_file)
    }


    val base_rdd = sc.textFile(base_instances_file).map({
      rs =>
        val line_list = rs.split("\t")
        (line_list(0), line_list(1).toLong)
    })

    val min_map = base_rdd.map({
      rs =>
        ("min", rs._2)
    }).reduceByKey((x, y) => if (x < y) x else y).collectAsMap()
    val min = min_map("min")
    println("min idx of base instances file =" + min)
    val incremental_idx = min - 1

    val base_map = base_rdd.collectAsMap()

    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_0)
    val instances_file = des_dir + "/" + curr_date + "-inc-" + time_id +  "-instances-all"
    if (!exists_hdfs_path(instances_file + "/_SUCCESS")) {
      delete_hdfs_path(instances_file)

      val incremental_rdd = importedDf.rdd.map(
        rs => {
          val idx2 = rs.getSeq[Long](0)
          val idx1 = rs.getSeq[Long](1)
          val idx_arr = rs.getSeq[Long](2)
          val idx0 = rs.getSeq[Long](3)
          val sample_idx = rs.getLong(4)
          val label_arr = rs.getSeq[Long](5)
          val dense = rs.getSeq[Long](6)

          val output: Array[String] = new Array[String](dense.length + idx_arr.length)
          for (idx <- dense.indices) {
            output(idx) = dense(idx).toString
          }
          for (idx <- idx_arr.indices) {
            output(idx + dense.length) = idx_arr(idx).toString
          }
          output.mkString("\t")
      }).flatMap(
        rs => {
          val line = rs.split("\t")
          for (elem <- line)
            yield (elem, 1L)
        }
      ).reduceByKey(_ + _).map ({
      case (key, value) =>
        (key, incremental_idx)
      }).filter(
        rs =>
          if (base_map.contains(rs._1)) {
            false
          } else {
            true
          }
      )

      base_rdd.union(incremental_rdd).sortBy(_._2 * -1).map {
        case (key, value) =>
          key + "\t" + value.toString
      }.repartition(1).saveAsTextFile(instances_file)

    }
  }
}
