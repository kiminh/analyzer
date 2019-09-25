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

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

/**
  * 解析tfrecord到hdfs并统计区间sparse feature出现的值和做映射以及负采样
  * created time : 2019/07/30 10:38
  * @author fenghuabin
  * @version 1.0
  *
  */

object MakeTrainExamplesNoUid {



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

  def GetDataRange(beginStr: String, endStr: String, format : String = "yyyy-MM-dd"): ArrayBuffer[String] = {
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
    if (args.length != 9) {
      System.err.println(
        """
          |you have to input 6 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(src_dir, date_begin, date_end, des_dir, instances_file, test_data_src, test_data_des, test_data_week, with_uid_des_dir) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //val src_date_list = src_date_str.split(";")
    val src_date_list = ArrayBuffer[String]()
    val src_week_list = ArrayBuffer[String]()
    val src_date_list_with_week = GetDataRange(date_begin, date_end)
    for (pair <- src_date_list_with_week) {
      src_date_list += pair.split(";")(0)
      src_week_list += pair.split(";")(1)
    }
    println("with_uid_des_dir:" + with_uid_des_dir)
    println("src_date_list:" + src_date_list.mkString(";"))
    println("src_week_list:" + src_week_list.mkString(";"))
    println("src_date_list_with_week:" + src_date_list_with_week.mkString("|"))

    /************************load map********************************/
    println("Load Non Uid SparseMap")
    val instances_all_map_others = with_uid_des_dir + "/" + instances_file + "-non-uid-indexed"
    val sparseMapOthers = sc.textFile(instances_all_map_others).map{
      rs => {
        val line = rs.split("\t")
        val field = line(0)
        val key = (line(1).toLong - 1L).toString
        (field, key)
      }
    }.collectAsMap()
    println("sparseMapOthers.size=" + sparseMapOthers.size)

    /************check sid************************/
    //println("Check Sample Index")
    //for (date_idx <- src_date_list.indices) {
    //  val src_date = src_date_list(date_idx)
    //  val src_week = src_week_list(date_idx)
    //  val tf_text = des_dir + "/" + src_date + "-text"
    //  if (exists_hdfs_path(tf_text)) {
    //    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    //    println("date:" + src_date)
    //    val rdd = sc.textFile(tf_text).map(
    //      f = rs => {
    //        val line_list = rs.split("\t")
    //        (line_list(0), 1)
    //      }
    //    )
    //    println("text lines:" + rdd.count.toString)
    //    val reduce_lines = rdd.reduceByKey(_ + _).count
    //    println("reduced lines:" + reduce_lines)
    //  }
    //}
    //println("Done.......")


    println("Do Mapping Non Uid Features")
    for (date_idx <- src_date_list.indices) {
      val src_date = src_date_list(date_idx)
      //val src_week = src_week_list(date_idx)
      val tf_text = with_uid_des_dir + "/" + src_date + "-text"
      val tf_text_mapped = des_dir + "/" + src_date + "-text-mapped"
      if (!exists_hdfs_path(tf_text_mapped + "/_SUCCESS") && exists_hdfs_path(tf_text)) {
        delete_hdfs_path(tf_text_mapped)
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("make ult mapped train files:" + tf_text_mapped)
        val ult_rdd = sc.textFile(tf_text).map(
          rs => {
            val line_list = rs.split("\t")
            val sid = line_list(0)
            val label = line_list(1)
            val label_arr = line_list(2)
            val dense = line_list(3).split(";")
            val idx0 = line_list(4)
            val idx1 = line_list(5)
            val idx2 = line_list(6)
            val idx_arr = line_list(7).split(";")
            val value_list = dense.slice(0, 25) ++ dense.slice(26, 28)
            val mapped_one_hot = value_list.map(x => sparseMapOthers.getOrElse(x, "-1"))
            val mapped_multi_hot = idx_arr.map(x => sparseMapOthers.getOrElse(x, "-1"))

            val ult_list:Array[String] = new Array[String](8)
            ult_list(0) = sid
            ult_list(1) = label
            ult_list(2) = label_arr
            ult_list(3) = mapped_one_hot.mkString(";")
            ult_list(4) = idx0
            ult_list(5) = idx1
            ult_list(6) = idx2
            ult_list(7) = mapped_multi_hot.mkString(";")
            ult_list.mkString("\t")
          }
        )
        val ult_rdd_count = ult_rdd.count
        println(s"ult_rdd_count : $ult_rdd_count")
        ult_rdd.repartition(1000).saveAsTextFile(tf_text_mapped)

        //保存count文件
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, ult_rdd_count)
        s"hadoop fs -put $fileName $tf_text_mapped/count" !

      }
    }
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

    println("Do Mapping Test Examples' features")
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    val test_file_src = src_dir + "/" + test_data_src
    val test_file_text_mapped = des_dir + "/" + test_data_des + "-text-mapped"
    if (!exists_hdfs_path(test_file_text_mapped)) {
      val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(test_file_src)
      println("DF file count:" + importedDf.count().toString + " of file:" + test_file_src)
      val mapped_rdd = importedDf.rdd.map(
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
          output
        }
      ).map(
        line_list => {
          val sid = line_list(0)
          val label = line_list(1)
          val label_arr = line_list(2)
          val dense = line_list(3).split(";")
          val idx0 = line_list(4)
          val idx1 = line_list(5)
          val idx2 = line_list(6)
          val idx_arr = line_list(7).split(";")

          val value_list = dense.slice(0, 25) ++ dense.slice(26, 28)
          val mapped_one_hot = value_list.map(x => sparseMapOthers.getOrElse(x, "-1"))
          val mapped_multi_hot = idx_arr.map(x => sparseMapOthers.getOrElse(x, "-1"))

          val ult_list:Array[String] = new Array[String](8)
          ult_list(0) = sid
          ult_list(1) = label
          ult_list(2) = label_arr
          ult_list(3) = mapped_one_hot.mkString(";")
          ult_list(4) = idx0
          ult_list(5) = idx1
          ult_list(6) = idx2
          ult_list(7) = mapped_multi_hot.mkString(";")
          ult_list.mkString("\t")
        }
      )
      val mapped_rdd_count = mapped_rdd.count
      println(s"mapped_rdd_count : $mapped_rdd_count")
      mapped_rdd.repartition(60).saveAsTextFile(test_file_text_mapped)
    }

    val test_file_text_mapped_tf = des_dir + "/" + test_data_des + "-text-mapped-tf"
    if (!exists_hdfs_path(test_file_text_mapped_tf) && exists_hdfs_path(test_file_text_mapped)) {
      val test_text_rdd = sc.textFile(test_file_text_mapped).map({
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

      val test_text_rdd_count = test_text_rdd.count
      println(s"test_text_rdd_count is : $test_text_rdd_count")

      val test_text_df: DataFrame = spark.createDataFrame(test_text_rdd, schema_new)
      test_text_df.repartition(60).write.format("tfrecords").option("recordType", "Example").save(test_file_text_mapped_tf)
    }
    println("Done.......")


    /************down sampling************************/
    println("Down Sampling")
    val negativeSampleRatio = 0.19
    for (date_idx <- src_date_list.indices) {
      val src_date = src_date_list(date_idx)
      val src_week = src_week_list(date_idx)
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val tf_text_mapped = des_dir + "/" + src_date + "-text-mapped"
      val tf_text_mapped_tf = des_dir + "/" + src_date + "-text-mapped-tf"
      //val tf_text_mapped_sampled = des_dir + "/" + src_date + "-text-mapped-sampled"
      val tf_text_mapped_sampled_tf = des_dir + "/" + src_date + "-text-mapped-tf-sampled"
      if (exists_hdfs_path(tf_text_mapped) && (!exists_hdfs_path(tf_text_mapped_sampled_tf))) {
        println("tf_text_mapped:" + tf_text_mapped)
        println("tf_text_mapped_sampled_tf:" + tf_text_mapped_sampled_tf)
        delete_hdfs_path(tf_text_mapped_tf)
        //delete_hdfs_path(tf_text_mapped_sampled)
        delete_hdfs_path(tf_text_mapped_sampled_tf)
        val tf_text_mapped_collect = tf_text_mapped + "/part*"
        println("now load data frame:" + tf_text_mapped_collect)
        val text_rdd = sc.textFile(tf_text_mapped_collect).map({
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

        val text_rdd_count = text_rdd.count
        println(s"text_rdd_count is : $text_rdd_count")

        val text_df: DataFrame = spark.createDataFrame(text_rdd, schema_new)
        text_df.repartition(500).write.format("tfrecords").option("recordType", "Example").save(tf_text_mapped_tf)

        //保存count文件
        //val text_df_count = text_df.count()
        //println(s"text_df_count is : $text_df_count")
        var fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, text_rdd_count)
        s"hadoop fs -put $fileName $tf_text_mapped_tf/count" !

        val sampled_rdd = text_rdd.filter(
          rs => {
            val label = rs.getFloat(1)
            var filter = false
            if (label > 0.0 || Random.nextFloat() < math.abs(negativeSampleRatio)) {
              filter = true
            }
            filter
          }
        )

        val sampled_rdd_count = sampled_rdd.count
        println(s"sampled_rdd_count is : $sampled_rdd_count")

        //Save DataFrame as TFRecords
        val sampled_df: DataFrame = spark.createDataFrame(sampled_rdd, schema_new)
        sampled_df.repartition(100).write.format("tfrecords").option("recordType", "Example").save(tf_text_mapped_sampled_tf)

        //保存count文件
        val sampled_df_count = sampled_df.count()
        println(s"sampled_df_count is : $sampled_df_count")
        fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, sampled_df_count)
        s"hadoop fs -put $fileName $tf_text_mapped_sampled_tf/count" !
      }
    }
    println("Done.......")
  }
}