package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

/**
  * 聚合adlistv4tfrecord特征
  * created time : 2019/09/22 11:31
  * @author fenghuabin
  * @version 1.0
  *
  */

object AggrAdListV4Samples {


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

  def GetDateRange(beginStr: String, endStr: String, format : String = "yyyy-MM-dd"): ArrayBuffer[String] = {
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
    if (args.length != 6) {
      System.err.println(
        """
          |you have to input 6 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(des_dir, src_date_list, src_file_list, date_last, date_curr, date_begin_strs) = args

    //println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("Aggr Examples")

    val date_list = src_date_list.split(";")
    val file_list = src_file_list.split(";")

    for (list_idx <- date_list.indices) {

      val curr_date = date_list(list_idx)
      val curr_file = file_list(list_idx)
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      println(s"curr_date : $curr_date")
      println(s"curr_file : $curr_file")

      val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file)
      //println("DF file count:" + importedDf.count().toString + " of train files")

      val file_des = des_dir + "/" + curr_date + "-aggr"
      if (!exists_hdfs_path(file_des + "/_SUCCESS") || !exists_hdfs_path(file_des + "/count")) {
        delete_hdfs_path(file_des)
        val text_train_rdd = importedDf.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx0 = rs.getSeq[Long](3)
            val sample_idx = rs.getLong(4)
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            val output = scala.collection.mutable.ArrayBuffer[String]()
            output += sample_idx.toString
            output += label_arr.map(_.toString).mkString(";")
            output += dense.map(_.toString).mkString(";")
            output += idx0.map(_.toString).mkString(";")
            output += idx1.map(_.toString).mkString(";")
            output += idx2.map(_.toString).mkString(";")
            output += idx_arr.map(_.toString).mkString(";")
            output
          }
        ).map({
          rs =>
            val sample_idx = rs(0).toLong
            val label_arr = rs(1).split(";").map(_.toLong).toSeq
            val dense = rs(2).split(";").map(_.toLong).toSeq
            val idx0 = rs(3).split(";").map(_.toLong).toSeq
            val idx1 = rs(4).split(";").map(_.toLong).toSeq
            val idx2 = rs(5).split(";").map(_.toLong).toSeq
            val idx_arr = rs(6).split(";").map(_.toLong).toSeq
            Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
        })

        val text_train_rdd_count = text_train_rdd.count
        println(s"text_train_rdd_count is : $text_train_rdd_count")

        val tf_df: DataFrame = spark.createDataFrame(text_train_rdd, schema_new)
        tf_df.repartition(2000).write.format("tfrecords").option("recordType", "Example").save(file_des)

        //保存count文件
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, text_train_rdd_count)
        s"hadoop fs -put $fileName $file_des/count" !

        s"hadoop fs -chmod -R 0777 $file_des" !
      }

      val instances_file = des_dir + "/" + curr_date + "-instances"
      if (!exists_hdfs_path(instances_file + "/_SUCCESS")) {
        importedDf.rdd.map(
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
          }
        ).flatMap(
          rs => {
            val line = rs.split("\t")
            for (elem <- line)
              yield (elem, 1L)
          }
        ).reduceByKey(_ + _).sortBy(_._2 * -1).map {
          case (key, value) =>
            key + "\t" + value.toString
        }.repartition(1).saveAsTextFile(instances_file)
      }
    }

    val instances_file = des_dir + "/" + date_last + "-instances"
    val base_map_file = des_dir + "/" + date_last + "-base-map-all"
    if (exists_hdfs_path(base_map_file + "/_SUCCESS") && exists_hdfs_path(instances_file + "/_SUCCESS")) {
      val base_rdd = sc.textFile(base_map_file).map({
        rs =>
          val line_list = rs.split("\t")
          (line_list(0), line_list(1).toLong)
      })

      val max_map = base_rdd.map({
        rs =>
          ("max", rs._2)
      }).reduceByKey((x, y) => if (x < y) y else x).collectAsMap()
      val max = max_map("max")
      println("max idx of base map file =" + max)
      val incremental_idx = max + 1

      val map_file = des_dir + "/" + date_curr + "-base-map-all"
      if (!exists_hdfs_path(map_file + "/_SUCCESS")) {
        delete_hdfs_path(map_file)
        val incremental_rdd = sc.textFile(instances_file).map({
          rs =>
            val line_list = rs.split("\t")
            (line_list(0), line_list(1).toLong)
        }).reduceByKey(_ + _).map ({
          case (key, _) =>
            (key, incremental_idx)
        })

        base_rdd.union(incremental_rdd).reduceByKey((x, y) => if (x >= y) y else x).repartition(1).sortBy(_._2).map {
          case (key, value) =>
            key + "\t" + value.toString
        }.saveAsTextFile(map_file)
      }
    }


    /**val date_begin_list = date_begin_strs.split(";")
    for (curr_begin_date <- date_begin_list) {
      val instances_all = des_dir + "/" + date_last + "_" + curr_begin_date + "-instances-all"
      val instances_map = des_dir + "/" + date_last + "_" + curr_begin_date + "-instances-map"
      if (!exists_hdfs_path(instances_all + "/_SUCCESS") || !exists_hdfs_path(instances_map + "/_SUCCESS")) {
        delete_hdfs_path(instances_all)
        delete_hdfs_path(instances_map)
        val instances_date_collect = GetDateRange(curr_begin_date, date_last)
        val output = ArrayBuffer[String]()
        for (curr_date <- instances_date_collect) {
          val instances_file = des_dir + "/" + curr_date + "-instances"
          if (exists_hdfs_path(instances_file + "/_SUCCESS")) {
            output += instances_file + "/part-*"
          }
        }

        if (instances_date_collect.size == output.size) {
          sc.textFile(output.mkString(",")).map({
            rs =>
              val line_list = rs.split("\t")
              (line_list(0), line_list(1).toLong)
          }).reduceByKey(_ + _).repartition(1).sortBy(_._2 * -1).map({
            case (key, value) =>
              key + "\t" + value.toString
          }).saveAsTextFile(instances_all)

          val acc = new LongAccumulator
          spark.sparkContext.register(acc)
          sc.textFile(instances_all).coalesce(1, false).map{
            rs => {
              acc.add(1L)
              val line = rs.split("\t")
              val key = line(0)
              (key, acc.count)
            }
          }.repartition(1).sortBy(_._2).map{
            case (key, value) => key + "\t" + value.toString
          }.saveAsTextFile(instances_map)
        }

      }
    }**/

  }
}

