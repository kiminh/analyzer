package com.cpc.spark.ml.dnn.baseData

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.sys.process._
import scala.util.Random
import org.apache.spark.util.LongAccumulator

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DateUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 解析tfrecord到hdfs
  * created time : 2019/07/13 16:38
  * @author fenghuabin
  * @version 1.0
  *
  */

object FeatureMonitor{


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
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(one_hot_feature_names, src_dir, sta_date, cur_date, begin_date, des_dir, numPartitions, count_one_hot, count_muti_hot) = args

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

    val src_date_list = sta_date.split(";")
    println("collect sparse feature instances")
    /************collect map instances for id feature************************/
    for (src_date <- src_date_list) {
      val instances_path_success = des_dir + "/" + src_date + "-instances/_SUCCESS"
      val curr_file_src_single = src_dir + "/" + src_date + "/part-r-00000"
      if (exists_hdfs_path(curr_file_src_single) && !exists_hdfs_path(instances_path_success)) {
        val instances_path = des_dir + "/" + src_date + "-instances"
        val curr_file_src = src_dir + "/" + src_date + "/part-r-*"
        s"hadoop fs -rm -r $instances_path" !
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src)
        println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src)
        if (importedDf.count() < 10000) {
          println("invalid df count, df file:" + curr_file_src)
        } else {
          val date_token = src_date.split("-")
          val viewName = "sql_table_view_name_" + date_token.mkString("_")
          println("viewName:" + viewName)
          importedDf.createOrReplaceTempView(viewName)
          val tf_decode_df_rows = spark.sql("SELECT sample_idx, label, dense, idx0, idx1, idx2, id_arr FROM " + viewName)
          tf_decode_df_rows.rdd.map(
            rs => {
              val sample_idx = rs.getLong(0).toString
              val label_arr = rs.getSeq[Long](1)
              val dense = rs.getSeq[Long](2)
              val idx0 = rs.getSeq[Long](3)
              val idx1 = rs.getSeq[Long](4)
              val idx2 = rs.getSeq[Long](5)
              val idx_arr = rs.getSeq[Long](6)

              val output_one_hot: Array[String] = new Array[String](dense.length)
              val output_multi_hot: Array[String] = new Array[String](idx_arr.length)

              var label = "1"
              if (label_arr.head == 1) {
                label = "1"
              } else {
                label = "0"
              }

              for (idx <- dense.indices) {
                output_one_hot(idx) = dense(idx).toString
              }

              for (idx <- idx_arr.indices) {
                output_multi_hot(idx) = idx_arr(idx).toString
              }

              label + "\t" + output_one_hot.mkString(";") + "\t" + output_multi_hot.mkString(";")
            }
          ).saveAsTextFile(instances_path)
        }
      }
    }

    for (src_date <- src_date_list) {
      val instances_path = des_dir + "/" + src_date + "-instances"
      val instances_path_success = des_dir + "/" + src_date + "-instances/_SUCCESS"
      val instance_path_res = instances_path + "-by-name"
      if (exists_hdfs_path(instances_path_success) && !exists_hdfs_path(instance_path_res)) {
          var data = sc.parallelize(Array[(String, Long)]())
          data = data.union(
            sc.textFile(instances_path).map(
              rs => {
                val line_list = rs.split("\t")
                val label = line_list(0)
                val one_hot_list = line_list(1).split(";")
                val muti_hot_list = line_list(2).split(";")

                val output: Array[String] = new Array[String](one_hot_list.length + muti_hot_list.length)

                for (idx <- one_hot_list.indices) {
                  output(idx) = idx.toString + ":" + one_hot_list(idx)
                }

                for (idx <- muti_hot_list.indices) {
                  output(idx + one_hot_list.length) = "666:" + muti_hot_list(idx)
                }
                output.mkString("\t")
              }
            ).flatMap(
              rs => {
                val line = rs.split("\t")
                for (elem <- line)
                  yield (elem, 1L)
              }
            ).reduceByKey(_ + _)
          )

          val instance_rdd = data.reduceByKey(_ + _)

          //save one hot feature instances
          for (idx <- 0 until count_one_hot.toInt) {
            val instance_path_by_feature = instances_path + "-by-name/" + name_list_one_hot(idx)
            val broadcast_idx = sc.broadcast(idx)
            instance_rdd.filter(
                rs => {
                  val feature_idx = rs._1.split(":")(0).toInt
                  var flag = false
                  if(feature_idx == broadcast_idx.value) { flag = true }
                  flag
                }
            ).map {
              case (key, value) =>
                (key.split(":")(1), value)
            }.repartition(1).sortBy(_._2 * -1).saveAsTextFile(instance_path_by_feature)
          }

          //save multi hot feature instances
          val instance_path_by_feature = instances_path + "-by-name/multi_hot_features"
          instance_rdd.filter(
            rs => {
              val feature_idx = rs._1.split(":")(0)
              var flag = false
              if(feature_idx == "666") { flag = true }
              flag
            }
          ).map {
            case (key, value) =>
              (key.split(":")(1), value)
          }.repartition(1).sortBy(_._2 * -1).saveAsTextFile(instance_path_by_feature)
        }
    }

    val dateArray = GetDataRange(begin_date, cur_date)
    println(dateArray)
    val monitor_path = des_dir + "/" + cur_date + "-monitor"
    for (idx <- 0 until count_one_hot.toInt) {
      val feature_name = name_list_one_hot(idx)
      val cur_feature_instances = monitor_path + "/instances/" + feature_name
      if (!exists_hdfs_path(cur_feature_instances)) {
        val instances_list = ArrayBuffer[String]()
        for (past_date <- dateArray) {
          val instances_path = des_dir + "/" + past_date + "-instances"
          val instance_path_by_feature = instances_path + "-by-name/" + feature_name
          instances_list += instance_path_by_feature

        }
        println("now collect instances set:" + instances_list.mkString(","))
        var data = sc.parallelize(Array[(String, Long)]())
        data = data.union(
          sc.textFile(instances_list.mkString(",")).map(
            rs => {
              val line_list = rs.substring(1, rs.length - 2).split(",")

              if (line_list.length == 2) {
                val feature_value = line_list(0)
                val feature_count = line_list(1)
                (feature_value, feature_count.toLong)
              } else {
                (rs, -1L)
              }


            }
          ).reduceByKey(_ + _)
        )

        data.reduceByKey(_ + _).repartition(1)
          .sortBy(_._2 * -1).map({case(key,value) => key + "\t" + value})
          .saveAsTextFile(cur_feature_instances)
      }
    }

  }
}
