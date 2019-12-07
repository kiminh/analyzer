package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.UserDefinedGenerator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
  * 解析tfrecord到hdfs并监控特征变化
  * created time : 2019/12/05 16:38
  * @author duanguangdong
  * @version 2.0
  *
  */

object FeatureMonitor {

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

  def GetDataRange(beginStr: String, endStr: String, format: String = "yyyy-MM-dd"): ArrayBuffer[String] = {
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
  def countIdNumber(): UserDefinedFunction = udf{
    row: Seq[Long] =>
      row

  }
  def getOneHotFeatureCount(RawData: DataFrame, count_one_hot: Int): String = {
    val result = RawData.rdd.map{
      r =>
        val dense = r.getAs[Seq[Long]]("dense")

    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      System.err.println(
        """
          |you have to input 8 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(one_hot_feature_names, src_dir, cur_date, begin_date, des_dir, numPartitions, count_one_hot, count_muti_hot) = args

    Logger.getRootLogger.setLevel(Level.WARN)
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val name_list_one_hot = one_hot_feature_names.split(",")
    val one_hot_hash = Seq[]
    if (count_one_hot.toInt != name_list_one_hot.length) {
      println("mismatched, count_one_hot:%d" + count_one_hot + ", name_list_one_hot.length:" + name_list_one_hot.length.toString)
      System.exit(1)
    }
    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/2019-12-03/part*9")
    val sample_count = importedDf.count()
    var one_hot_feature_count = Seq[Long]()
    var one_hot_feature_id_num = Seq[Long]()
    for(i <- 0 until count_one_hot.toInt){
      one_hot_feature_count += importedDf.select(s"dense[$i]").filter(_ != one_hot_hash[i]).count()
      one_hot_feature_id_num += importedDf.select(s"dense[$i]").filter(_ != one_hot_hash[i]).distinct().count()
    }
    val feature_count = importedDf.rdd.map{
      r =>
        val sample_idx = r.getAs[Long]("sample_idx")
        val lable = r.getAs[Seq[Long]]("lable")
        val dense = r.getAs[Seq[Long]]("dense")
        val idx0 = r.getAs[Seq[Long]]("idx0")
        val idx1 = r.getAs[Seq[Long]]("idx1")
        val idx2 = r.getAs[Seq[Long]]("idx2")
        val id_arr = r.getAs[Seq[Long]]("id_arr")

    }

  }
}
