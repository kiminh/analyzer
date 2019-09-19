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

object MakeAdListV4Samples {


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
    if (args.length != 5) {
      System.err.println(
        """
          |you have to input 8 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(des_dir, train_files, test_file, curr_date, time_id) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val idealIdMap = scala.collection.mutable.HashMap.empty[String, Int]
    val unitIdMap = scala.collection.mutable.HashMap.empty[String, Int]
    val planIdMap = scala.collection.mutable.HashMap.empty[String, Int]
    val userIdMap = scala.collection.mutable.HashMap.empty[String, Int]

    val idealIdMapBC = sc.broadcast(idealIdMap)
    val unitIdMapBC = sc.broadcast(unitIdMap)
    val planIdMapBC = sc.broadcast(planIdMap)
    val userIdMapBC = sc.broadcast(userIdMap)

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("Extract Test Examples' AD Info")
    val text_test = des_dir + "/" + curr_date + "-" + time_id + "-test"

    if (exists_hdfs_path(text_test)) {
      delete_hdfs_path(text_test)
    }

    val importedDfTest: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(test_file)
    println("DF file count:" + importedDfTest.count().toString + " of file:" + test_file)
    importedDfTest.printSchema()
    importedDfTest.show(3)
    val totalMap = importedDfTest.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        //10,11,12,13
        //,ideaid,unitid,planid,userid
        //(dense(10).toString, dense(11).toString, dense(12).toString, dense(13).toString)
        val output = scala.collection.mutable.ArrayBuffer[String]()
        output += dense(10).toString
        output += dense(11).toString
        output += dense(12).toString
        output += dense(13).toString
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("totalMap.size=" + totalMap.size)

    //.repartition(100).saveAsTextFile(text_test)
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("Filter Train Examples")
    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))

    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files)
    println("DF file count:" + importedDf.count().toString + " of train files")
    val text_train_rdd = importedDf.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val idealId = dense(10).toString
        val unitId = dense(11).toString
        val planId = dense(12).toString
        val userId = dense(13).toString

        val output = scala.collection.mutable.ArrayBuffer[String]()
        output += sample_idx.toString
        output += label_arr.map(_.toString).mkString(";")
        output += dense.map(_.toString).mkString(";")
        output += idx0.map(_.toString).mkString(";")
        output += idx1.map(_.toString).mkString(";")
        output += idx2.map(_.toString).mkString(";")
        output += idx_arr.map(_.toString).mkString(";")
        (idealId, unitId, planId, userId, output.mkString("\t"))
      }
    ).filter(
      rs => {
        val idealId = rs._1
        val unitId = rs._2
        val planId = rs._3
        val userId = rs._4
        var filter = true
        if (!totalMap.contains(idealId)) {
          filter = false
        }
        if (!totalMap.contains(unitId)) {
          filter = false
        }
        if (!totalMap.contains(planId)) {
          filter = false
        }
        if (!totalMap.contains(userId)) {
          filter = false
        }
        filter
      }
    ).map(
      rs => {
        rs._5
      }
    )

    val text_train_rdd_count = text_train_rdd.count
    println(s"filter text_train_rdd_count is : $text_train_rdd_count")

    val tf_train_rdd = text_train_rdd.map({
      rs =>
        val rs_list = rs.split("\t")
        val sample_idx = rs_list(0).toLong
        val label_arr = rs_list(1).split(";").map(_.toLong).toSeq
        val dense = rs_list(2).split(";").map(_.toLong).toSeq
        val idx0 = rs_list(3).split(";").map(_.toLong).toSeq
        val idx1 = rs_list(4).split(";").map(_.toLong).toSeq
        val idx2 = rs_list(5).split(";").map(_.toLong).toSeq
        val idx_arr = rs_list(6).split(";").map(_.toLong).toSeq
        Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
    })

    //val tf_train_rdd_count = tf_train_rdd.count
    //println(s"tf_train_rdd_count is : $tf_train_rdd_count")

    val text_train = des_dir + "/" + curr_date + "-" + time_id + "-train-text"
    val tf_train = des_dir + "/" + curr_date + "-" + time_id + "-train-tf"

    if (exists_hdfs_path(text_train)) {
      delete_hdfs_path(text_train)
    }

    if (exists_hdfs_path(tf_train)) {
      delete_hdfs_path(tf_train)
    }

    val tf_df: DataFrame = spark.createDataFrame(tf_train_rdd, schema_new)
    tf_df.repartition(100).write.format("tfrecords").option("recordType", "Example").save(tf_train)

    //保存count文件
    val fileName = "count_" + Random.nextInt(100000)
    writeNum2File(fileName, text_train_rdd_count)
    s"hadoop fs -put $fileName $tf_train/count" !

    s"hadoop fs -chmod -R 0777 $tf_train" !

    text_train_rdd.repartition(100).saveAsTextFile(text_train)
    s"hadoop fs -put $fileName $text_train/count" !

    s"hadoop fs -chmod -R 0777 $text_train" !

  }
}

