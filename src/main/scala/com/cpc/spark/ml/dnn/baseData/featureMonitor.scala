package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.UserDefinedGenerator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

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
  Logger.getRootLogger.setLevel(Level.WARN)

  def countIdNumber(multi_hot_num: Int): UserDefinedFunction = udf{
    row: Seq[Long] =>
      val result_map = row.groupBy(identity).mapValues(_.size)
      var result = Seq[Long]()
      for(i <- 0 until multi_hot_num){
        result = result :+ result_map(i).toLong
      }
      result
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      System.err.println(
        """
          |you have to input 8 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val Array(one_hot_feature, count_one_hot, count_multi_hot, model_name, sample_path, update_type, curday, hour) = args
    val spark = SparkSession.builder().appName("feature monitor").enableHiveSupport().getOrCreate()
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val name_list_one_hot = one_hot_feature.split(",")
    if (count_one_hot.toInt != name_list_one_hot.length) {
      println("mismatched, count_one_hot:%d" + count_one_hot + ", name_list_one_hot.length:" + name_list_one_hot.length.toString)
      System.exit(1)
    }
    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(s"$sample_path")
    importedDf.cache()
    val sample_count = importedDf.count()
    //统计one-hot特征的非空样本占比，以及每个one-hot特征的id量；
    var feature_defalt = Murmur3Hash.stringHash64(name_list_one_hot(0), 0)
    var one_hot_feature_count = (importedDf.select(expr(s"dense[0]").alias("feature")).filter(s"feature!=$feature_defalt").count()*1.0/sample_count).toString
    var one_hot_feature_id_num = importedDf.select(expr(s"dense[0]").alias("feature")).filter(s"feature!=$feature_defalt").distinct().count().toString
    import spark.implicits._
    for(i <- 1 until count_one_hot.toInt){
      feature_defalt = Murmur3Hash.stringHash64(name_list_one_hot(i), 0)
      one_hot_feature_count = one_hot_feature_count + "," + (importedDf.select(expr(s"dense[$i]").alias("feature")).filter(s"feature!=$feature_defalt").count()*1.0/sample_count).toString
      one_hot_feature_id_num = one_hot_feature_id_num + "," + importedDf.select(expr(s"dense[$i]").alias("feature")).filter(s"feature!=$feature_defalt").distinct().count().toString
    }
    //统计每个multi-hot特征的平均id量
    val feature_count = importedDf.select(countIdNumber(15)($"idx1").alias("sparse"))
    var multi_hot_feature_count = feature_count.select(avg(expr(s"sparse[0]")).alias("feature")).rdd.map(r => r.getAs[Double]("feature")).collect()(0).toString

    for(i <- 1 until count_multi_hot.toInt){
      multi_hot_feature_count = multi_hot_feature_count + "," + feature_count.select(avg(expr(s"sparse[$i]")).alias("feature")).rdd.map(r => r.getAs[Double]("feature")).collect()(0).toString
    }
    spark.sql(
      s"""
         |insert into dl_cpc.cpc_feature_monitor partition(day="$curday")
         |select "$model_name", "$one_hot_feature_count", "$one_hot_feature_id_num", "$multi_hot_feature_count", "$hour"
         |""".stripMargin)
    var exception_feature = Seq[String]()
    if(update_type == "daily"){
      val feature_value = spark.sql(
        s"""
           |select one_hot_feature_count, one_hot_feature_id_num, multi_hot_feature_count
           |from dl_cpc.cpc_feature_monitor where day="$oneday" and model_name="$model_name"
           |""".stripMargin)
      val feature_count = feature_value.rdd.map(r => (r.getAs[String]("one_hot_feature_count"),r.getAs[String]("one_hot_feature_id_num"),r.getAs[String]("multi_hot_feature_count"))).collect()
      val one_hot_feature_count_his = feature_count(0)._1.split(",")
      val one_hot_feature_id_num_his = feature_count(0)._2.split(",")
      val multi_hot_feature_count_his = feature_count(0)._3.split(",")
      val one_hot_feature_count_cur = one_hot_feature_count.split(",")
      val one_hot_feature_id_num_cur = one_hot_feature_id_num.split(",")
      val multi_hot_feature_count_cur = multi_hot_feature_count.split(",")
      for(i <- 0 until one_hot_feature_count_his.length){
        if(one_hot_feature_count_his(i).toFloat/one_hot_feature_count_cur(i).toFloat>1.3 || one_hot_feature_count_cur(i).toFloat/one_hot_feature_count_his(i).toFloat>1.3){
          exception_feature = exception_feature :+ name_list_one_hot(i)
        }
      }
      for(i <- 0 until one_hot_feature_id_num_his.length){
        if(one_hot_feature_id_num_cur(i).toFloat/one_hot_feature_id_num_his(i).toFloat>1.3 || one_hot_feature_id_num_his(i).toFloat/one_hot_feature_id_num_cur(i).toFloat>1.3){
          exception_feature = exception_feature :+ name_list_one_hot(i)
        }
      }
      for(i <- 0 until multi_hot_feature_count_his.length){
        if(multi_hot_feature_count_cur(i).toFloat/multi_hot_feature_count_his(i).toFloat>1.3 || multi_hot_feature_count_his(i).toFloat/multi_hot_feature_count_cur(i).toFloat>1.3){
          exception_feature = exception_feature :+ i.toString
        }
      }
    }
    if(update_type == "hourly"){
      val feature_value = spark.sql(
        s"""
           |select one_hot_feature_count, one_hot_feature_id_num, multi_hot_feature_count
           |from dl_cpc.cpc_feature_monitor where day="$oneday" and model_name="$model_name" and hour='$hour'
           |""".stripMargin)
      val feature_count = feature_value.rdd.map(r => (r.getAs[String]("one_hot_feature_count"),r.getAs[String]("one_hot_feature_id_num"),r.getAs[String]("multi_hot_feature_count"))).collect()
      val one_hot_feature_count_his = feature_count(0)._1.split(",")
      val one_hot_feature_id_num_his = feature_count(0)._2.split(",")
      val multi_hot_feature_count_his = feature_count(0)._3.split(",")
      val one_hot_feature_count_cur = one_hot_feature_count.split(",")
      val one_hot_feature_id_num_cur = one_hot_feature_id_num.split(",")
      val multi_hot_feature_count_cur = multi_hot_feature_count.split(",")
      for(i <- 0 until one_hot_feature_count_his.length){
        if(one_hot_feature_count_his(i).toFloat/one_hot_feature_count_cur(i).toFloat>1.3 || one_hot_feature_count_cur(i).toFloat/one_hot_feature_count_his(i).toFloat>1.3){
          exception_feature = exception_feature :+ name_list_one_hot(i)
        }
      }
      for(i <- 0 until one_hot_feature_id_num_his.length){
        if(one_hot_feature_id_num_cur(i).toFloat/one_hot_feature_id_num_his(i).toFloat>1.3 || one_hot_feature_id_num_his(i).toFloat/one_hot_feature_id_num_cur(i).toFloat>1.3){
          exception_feature = exception_feature :+ name_list_one_hot(i)
        }
      }
      for(i <- 0 until multi_hot_feature_count_his.length){
        if(multi_hot_feature_count_cur(i).toFloat/multi_hot_feature_count_his(i).toFloat>1.3 || multi_hot_feature_count_his(i).toFloat/multi_hot_feature_count_cur(i).toFloat>1.3){
          exception_feature = exception_feature :+ i.toString
        }
      }
    }
    if(exception_feature.length>0){
      print("feature exception" + exception_feature.toString())
      System.exit(1)
    }
  }
}