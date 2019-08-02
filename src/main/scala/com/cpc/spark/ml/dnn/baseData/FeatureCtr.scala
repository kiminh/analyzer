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
import org.apache.commons.lang.StringUtils

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
      if (sdf.format(dateBegin) == "2019-06-25") {
        println("drop invalid src date 2019-06-25")
      } else {
        ranges += sdf.format(dateBegin) + ";" + DateFormatUtils.format(dateBegin, "E")
      }
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
      if (sdf.format(dateBegin) == "2019-06-25") {
        println("drop invalid src date 2019-06-25")
      } else {
        ranges += sdf.format(dateBegin)
      }
      dateBegin = DateUtils.addDays(dateBegin, 1)
    }
    ranges
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      System.err.println(
        """
          |you have to input 8 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val Array(one_hot_feature_names, src_dir, des_dir, feature_date_begin, feature_date_end, numPartitions, count_one_hot, count_multi_hot) = args

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

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val collect_date_end = feature_date_end
    val collect_date_begin = sdf.format(DateUtils.addDays(sdf.parse(feature_date_begin), -28))

    val collect_date_list_ori = ArrayBuffer[String]()
    val collect_week_list_ori = ArrayBuffer[String]()
    val collect_date_list_ori_with_week = GetDataRangeWithWeek(collect_date_begin, collect_date_end)
    for (pair <- collect_date_list_ori_with_week) {
      collect_date_list_ori += pair.split(";")(0)
      collect_week_list_ori += pair.split(";")(1)
    }
    val collect_date_list = collect_date_list_ori.reverse
    val collect_week_list = collect_week_list_ori.reverse

    println("collect_date_list:" + collect_date_list.mkString(";"))
    println("collect_week_list:" + collect_week_list.mkString(";"))
    println("collect_date_list_with_week:" + collect_date_list_ori_with_week.mkString("|"))

    val name_idx_map: mutable.Map[String, Int] = mutable.Map()
    for (idx <- name_list_one_hot.indices) {
      name_idx_map += (name_list_one_hot(idx) -> idx)
    }
    val name_idx_map_bc = sc.broadcast(name_idx_map)

    val cross_features_str ="sex,adtype,adclass,os,network,phone_price,brand,city_level,age,hour"
    val cross_features_list = cross_features_str.split(",")
    val cross_features_list_bc = sc.broadcast(cross_features_list)

    val cross_features_list_2 = ArrayBuffer[(String, String)]()
    for (idx <- 0 until cross_features_list.length) {
      for (inner <- (idx + 1) until cross_features_list.length) {
        cross_features_list_2 += ((cross_features_list(idx), cross_features_list(inner)))
      }
    }
    println("cross_features_list_2 len:" + cross_features_list_2.length)
    for (pair <- cross_features_list_2) {
      println(pair._1 + " X " + pair._2)
    }
    val cross_features_list_2_bc = sc.broadcast(cross_features_list_2)

    /** **********make ctr statistics collect ************************/
    println("Make ctr statistics collect")
    for (date_idx <- collect_date_list.indices) {
      val collect_date = collect_date_list(date_idx)
      val collect_week = collect_week_list(date_idx)
      println("date:" + collect_date + ", week:" + collect_week)
      val curr_file_src = src_dir + "/" + collect_date
      val tf_ctr_collect = des_dir + "/collect/" + collect_date + "-ctr-rate"

      if (exists_hdfs_path(curr_file_src) && !exists_hdfs_path(tf_ctr_collect + "/_SUCCESS")) {
        s"hadoop fs -rm -r $tf_ctr_collect" !

        val curr_file_src_collect = src_dir + "/" + collect_date + "/part-r-*"
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
            dense_str = dense.map(_.toString) ++ Seq[String](collect_week)

            var count = (0L, 0L)
            if (label_arr.head == 1L) {
              count = (1L, 1L)
            } else {
              count = (0L, 1L)
            }

            val output = scala.collection.mutable.ArrayBuffer[(String, (Long, Long))]()
            output += (("week\t" + collect_week, count))

            for (name <- cross_features_list_bc.value) {
              output += ((name + "\t" + dense(name_idx_map_bc.value(name)), count))
            }

            for (name_pair <- cross_features_list_2_bc.value) {
              val name = name_pair._1 + "x" + name_pair._2
              val value = dense(name_idx_map_bc.value(name_pair._1)) + "x" + dense(name_idx_map_bc.value(name_pair._2))
              output += ((name + "\t" + value, count))
            }

            output
          }
        ).flatMap(
          output_array => {
            for (elem <- output_array)
              yield elem
          }
        ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(
          {
            rs=>
              rs._1 + "\t" + rs._2._1 + "\t" + rs._2._2 + "\t" + (rs._2._1.toDouble/rs._2._2.toDouble)
          }
        ).repartition(1).saveAsTextFile(tf_ctr_collect)
      }
    }
    println("Done.......")


    /** **********make ctr feature************************/
    println("Make ctr feature")
    val date_list_ori = GetDataRange(feature_date_begin, feature_date_end)
    val date_list = date_list_ori.reverse
    println("make ctr featrues of date_list:" + date_list.mkString(";"))

    for (ctr_feature_date <- date_list) {
      val tf_ctr_feature = des_dir + "/" + ctr_feature_date
      if (!exists_hdfs_path(tf_ctr_feature + "/_SUCCESS")) {
        s"hadoop fs -rm -r $tf_ctr_feature" !

        println("++++++++++++++++++++++++++++++++++++++")
        println("make ctr feature of date:" + ctr_feature_date)
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val collect_date_end = sdf.format(DateUtils.addDays(sdf.parse(ctr_feature_date), -1))
        val collect_date_begin = sdf.format(DateUtils.addDays(sdf.parse(ctr_feature_date), -28))
        val collect_date_list = GetDataRange(collect_date_begin, collect_date_end)
        println("collect dates:" + collect_date_list.mkString(","))

        val valid_collect_file = ArrayBuffer[String]()
        for (collect_date <- collect_date_list.indices) {
          val tf_ctr_collect = des_dir + "/collect/" + collect_date + "-ctr-rate"
          if (exists_hdfs_path(tf_ctr_collect + "/_SUCCESS")) {
            valid_collect_file += tf_ctr_collect
          }
        }
        println("valid collect dates:" + valid_collect_file.mkString(","))

        sc.textFile(valid_collect_file.mkString(",")).map(
          {
            rs =>
              val line_list = rs.split("\t")
              if (line_list.length == 4) {
                val feature_name = StringUtils.split(line_list(0), "_")(0)
                val feature_value = StringUtils.split(line_list(0), "_")(1)
                (feature_name + "\t" + feature_value, (line_list(1).toLong, line_list(2).toLong))
              } else {
                (line_list(0) + "\t" + line_list(1), (line_list(2).toLong, line_list(3).toLong))
              }
          }
        ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(
          {
            rs=>
              rs._1 + "\t" + rs._2._1 + "\t" + rs._2._2 + "\t" + (rs._2._1.toDouble/rs._2._2.toDouble)
          }
        ).repartition(1).saveAsTextFile(tf_ctr_feature)
      }

    }

    println("Done.......")

  }
}
