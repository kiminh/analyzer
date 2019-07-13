package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.sys.process._
import scala.util.Random

/**
  * 生成特点集合中sparse id的全集
  * created time : 2019/07/13 16:31
  * @author fenghuabin
  * @version 1.0
  *
  */

object CollectTrainUniqSparseID {

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

  //def getColAtIndex(id:Int): Column = {
  //  col(s"column1")(id).as(s"column1_${id+1}")
  //}


  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println(
        """
          |you have to input 4 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val Array(src_dir, src_date_str, des_instances_file, numPartitions) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val src_date_list = src_date_str.split(";")
    val output = scala.collection.mutable.ArrayBuffer[String]()
    for (idx <- 0 until src_date_list.length) {
      val curr_file = src_dir + "/" + src_date_list(idx) + "-instances"
      if (exists_hdfs_path(curr_file)) {
        output(idx) = curr_file
      }
    }
    val src_instances_files = output.mkString(",")
    //统计每个ID特征的每个取值出现的次数
    if (!exists_hdfs_path(des_instances_file)) {
      var data = sc.parallelize(Array[(String, Int)]())
      data = data.union(
        sc.textFile(src_instances_files).map(
          rs => {
            val line = rs.split("\t")
            val key = line(0)
            val cnt = line(1).toInt
            (key, cnt)
          }
        ).reduceByKey(_ + _)
      )
      data.reduceByKey(_ + _).repartition(1).sortBy(_._2).map {
        case (key, value) =>
          key + "\t" + value.toString
      }.saveAsTextFile(des_instances_file)
    }
  }
}