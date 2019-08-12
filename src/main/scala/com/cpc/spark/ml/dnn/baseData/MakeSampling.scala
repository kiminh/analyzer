package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

/**
  * 解析tfrecord到hdfs并下采样
  * created time : 2019/07/13 10:38
  * @author fenghuabin
  * @version 1.0
  *
  */

object MakeSampling {



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
    if (args.length != 10) {
      System.err.println(
        """
          |you have to input 10 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(sta_date_begin, sta_date_end, src_dir, date_begin, date_end, des_dir, test_data_src, test_data_des, test_data_week, numPartitions) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //val src_date_list = src_date_str.split(";")
    val src_date_list = ArrayBuffer[String]()
    val src_week_list = ArrayBuffer[String]()
    val src_date_list_with_week = GetDataRangeWithWeek(date_begin, date_end)
    for (pair <- src_date_list_with_week) {
      src_date_list += pair.split(";")(0)
      src_week_list += pair.split(";")(1)
    }
    println("src_date_list:" + src_date_list.mkString(";"))
    println("src_week_list:" + src_week_list.mkString(";"))
    println("src_date_list_with_week:" + src_date_list_with_week.mkString("|"))


    /** **********make text examples ************************/
    println("Make text examples")
    for (date_idx <- src_date_list.indices) {
      val src_date = src_date_list(date_idx)
      val src_week = src_week_list(date_idx)
      val curr_file_src = src_dir + "/" + src_date
      val tf_text = des_dir + "/" + src_date + "-text"
      if (!exists_hdfs_path(tf_text) && exists_hdfs_path(curr_file_src)) {
        val curr_file_src_collect = src_dir + "/" + src_date + "/part-r-*"
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src_collect)
        //println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src_collect)
        //importedDf.printSchema()
        //importedDf.show(3)

        importedDf.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx0 = rs.getSeq[Long](3)
            val sample_idx = rs.getLong(4)
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            var label = "0.0"
            if (label_arr.head == 1L) {
              label = "1.0"
            }

            val output = scala.collection.mutable.ArrayBuffer[String]()
            output += sample_idx.toString
            output += label
            output += label_arr.map(_.toString).mkString(";")
            output += dense.map(_.toString).mkString(";")
            output += idx0.map(_.toString).mkString(";")
            output += idx1.map(_.toString).mkString(";")
            output += idx2.map(_.toString).mkString(";")
            output += idx_arr.map(_.toString).mkString(";")

            output.mkString("\t")
          }
        ).repartition(1000).saveAsTextFile(tf_text)
      }
    }
    println("Done.......")



    val sta_date_list = GetDataRange(sta_date_begin, sta_date_end)
    sta_date_list += "20190724"
    println("sta_date_list:" + src_date_list.mkString(";"))
    /** **********get real ctr************************/
    println("Get Real CTR")
    val real_ctr = des_dir + "/" + "real-ctr-" + sta_date_begin + "-" + sta_date_end
    var data = sc.parallelize(Array[(String, (Long, Long))]())
    for (date_idx <- sta_date_list.indices) {
      val sta_date = src_date_list(date_idx)
      val tf_text = des_dir + "/" + sta_date + "-text"
      if (exists_hdfs_path(tf_text + "/_SUCCESS")) {
        data = data.union(
          sc.textFile(tf_text).map(
            rs => {
              val line_list = rs.split("\t")
              val label = line_list(1)
              if (label == "1.0") {
                ("placeholder", (1L, 1L))
              } else {
                ("placeholder", (0L, 1L))
              }
            }
          ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        ).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      }
    }
    data.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).repartition(1).saveAsTextFile(real_ctr)
    println("Done.......")

    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label_single", FloatType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))
    /** **********down sampling ************************/
    println("Down Sampling")
    val negativeSampleRatio = 0.19
    for (date_idx <- src_date_list.indices) {
      val src_date = src_date_list(date_idx)
      val src_week = src_week_list(date_idx)
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val tf_text = des_dir + "/" + src_date + "-text"
      val tf_text_sampled = des_dir + "/" + src_date + "-text-sampled"
      val tf_text_sampled_tf = des_dir + "/" + src_date + "-text-sampled-tf"
      if (exists_hdfs_path(tf_text) && (!exists_hdfs_path(tf_text_sampled_tf + "/_SUCCESS"))) {
        println("tf_text:" + tf_text)
        println("tf_text_sampled:" + tf_text_sampled)
        println("tf_text_sampled_tf:" + tf_text_sampled_tf)
        delete_hdfs_path(tf_text_sampled)
        delete_hdfs_path(tf_text_sampled_tf)
        sc.textFile(tf_text).filter(
          rs => {
            val label = rs.split("\t")(1)
            var filter = false
            if (label == "1.0" || Random.nextFloat() < math.abs(negativeSampleRatio)) {
              filter = true
            }
            filter
          }
        ).repartition(100).saveAsTextFile(tf_text_sampled)

        val sampled_text_rdd = sc.textFile(tf_text_sampled).map({
          rs =>
            val rs_list = rs.split("\t")
            val sample_idx = rs_list(0).toLong
            val label = rs_list(1).toFloat
            val label_arr = rs_list(2).split(";").map(_.toLong).toSeq
            val dense = rs_list(3).split(";").map(_.toLong).toSeq
            val idx0 = rs_list(4).split(";").map(_.toLong).toSeq
            val idx1 = rs_list(5).split(";").map(_.toLong).toSeq
            val idx2 = rs_list(6).split(";").map(_.toLong).toSeq
            val idx_arr = rs_list(7).split(";").map(_.toLong).toSeq
            Row(sample_idx, label, label_arr, dense, idx0, idx1, idx2, idx_arr)
        })

        val sampled_text_rdd_count = sampled_text_rdd.count
        println(s"sampled_text_rdd_count is : $sampled_text_rdd_count")

        val sampled_text_df: DataFrame = spark.createDataFrame(sampled_text_rdd, schema_new)
        sampled_text_df.repartition(100).write.format("tfrecords").option("recordType", "Example").save(tf_text_sampled_tf)

        //保存count文件
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, sampled_text_rdd_count)
        s"hadoop fs -put $fileName $tf_text_sampled_tf/count" !

      }
    }
    println("Done.......")

    println("Collect Test Examples")
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    val test_file_src = src_dir + "/" + test_data_src
    val test_file_text = des_dir + "/" + test_data_des + "-text"
    val test_file_text_tf = des_dir + "/" + test_data_des + "-text-tf"
    if (!exists_hdfs_path(test_file_text_tf + "/_SUCCESS")) {
      s"hadoop fs -rm -r $test_file_text" !

      s"hadoop fs -rm -r $test_file_text_tf" !

      val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(test_file_src)
      println("DF file count:" + importedDf.count().toString + " of file:" + test_file_src)
      importedDf.rdd.map(
        rs => {
          val idx2 = rs.getSeq[Long](0)
          val idx1 = rs.getSeq[Long](1)
          val idx_arr = rs.getSeq[Long](2)
          val idx0 = rs.getSeq[Long](3)
          val sample_idx = rs.getLong(4)
          val label_arr = rs.getSeq[Long](5)
          val dense = rs.getSeq[Long](6)

          var label = "0.0"
          if (label_arr.head == 1L) {
            label = "1.0"
          }

          val output = scala.collection.mutable.ArrayBuffer[String]()
          output += sample_idx.toString
          output += label
          output += label_arr.map(_.toString).mkString(";")
          output += dense.map(_.toString).mkString(";")
          output += idx0.map(_.toString).mkString(";")
          output += idx1.map(_.toString).mkString(";")
          output += idx2.map(_.toString).mkString(";")
          output += idx_arr.map(_.toString).mkString(";")

          output.mkString("\t")
        }
      ).repartition(60).saveAsTextFile(test_file_text)

      val test_file_rdd = sc.textFile(test_file_text).map({
        rs =>
          val rs_list = rs.split("\t")
          val sample_idx = rs_list(0).toLong
          val label = rs_list(1).toFloat
          val label_arr = rs_list(2).split(";").map(_.toLong).toSeq
          val dense = rs_list(3).split(";").map(_.toLong).toSeq
          val idx0 = rs_list(4).split(";").map(_.toLong).toSeq
          val idx1 = rs_list(5).split(";").map(_.toLong).toSeq
          val idx2 = rs_list(6).split(";").map(_.toLong).toSeq
          val idx_arr = rs_list(7).split(";").map(_.toLong).toSeq
          Row(sample_idx, label, label_arr, dense, idx0, idx1, idx2, idx_arr)
      })

      val test_file_rdd_count = test_file_rdd.count
      println(s"test_file_rdd_count is : $test_file_rdd_count")

      val test_file_df: DataFrame = spark.createDataFrame(test_file_rdd, schema_new)
      test_file_df.repartition(60).write.format("tfrecords").option("recordType", "Example").save(test_file_text_tf)
    }

    println("Done.......")

  }
}