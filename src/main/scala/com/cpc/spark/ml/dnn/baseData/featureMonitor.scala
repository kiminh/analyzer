package com.cpc.spark.ml.dnn.baseData

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.BytesWritable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.tensorflow.example.Example
import org.tensorflow.spark.datasources.tfrecords.TensorFlowInferSchema
import org.tensorflow.spark.datasources.tfrecords.serde.DefaultTfRecordRowDecoder

import scala.collection.mutable

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
  def generateSql(model_name: String, update_type: String, hour: String, curday: String, sample_path: String, pt: String): String = {
    var sql = ""
    if(update_type == "daily"){
      sql = s"select example from $sample_path where dt='$curday' and pt='$pt' and task='$model_name'"
    } else if (update_type == "hourly"){
      sql = s"select example from $sample_path where dt='$curday' and pt='$pt' and task='$model_name' and hour='$hour'"
    } else if (update_type == "halfhourly" && hour.substring(2, 4) == "00"){
      val hour_new = hour.substring(0, 2)
      sql = s"select example from $sample_path where dt='$curday' and pt='realtime-00' and task='$model_name' and hour='$hour_new'"
    } else if (update_type == "halfhourly" && hour.substring(2, 4) == "30"){
      val hour_new = hour.substring(0, 2)
      sql = s"select example from $sample_path where dt='$curday' and pt='realtime-30' and task='$model_name' and hour='$hour_new'"
    }
    sql
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 12) {
      System.err.println(
        """
          |you have to input 8 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val Array(one_hot_feature, count_one_hot, count_multi_hot, model_name, sample_path, update_type, curday, hour, pt, onehot_count_threshold, onehot_idcount_threshold, multihot_count_threshold) = args
    val spark = SparkSession.builder().appName("feature monitor").enableHiveSupport().getOrCreate()
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val threshold1 = mutable.Map[String, Float]()
    val threshold2 = mutable.Map[String, Float]()
    val threshold3 = mutable.Map[String, Float]()
    if(onehot_count_threshold.split(",").length>0){
      for(i <- onehot_count_threshold.split(",")){
        threshold1.put(i.split(":")(0), i.split(":")(1).toFloat)
        if (count_one_hot.toInt <= i.split(":")(0).toInt) {
          println("out of boundary, onehot_count:%s" + i.split(":")(0))
          System.exit(1)
        }
      }
    }
    if(onehot_idcount_threshold.split(",").length>0){
      for(i <- onehot_idcount_threshold.split(",")){
        threshold2.put(i.split(":")(0), i.split(":")(1).toFloat)
        if (count_one_hot.toInt+1 <= i.split(":")(0).toInt) {
          println("out of boundary, onehot_idcount:%s" + i.split(":")(0))
          System.exit(1)
        }
      }
    }
    if(multihot_count_threshold.split(",").length>0){
      for(i <- multihot_count_threshold.split(",")){
        threshold3.put(i.split(":")(0), i.split(":")(1).toFloat)
        if (count_multi_hot.toInt <= i.split(":")(0).toInt) {
          println("out of boundary, count_multi_hot:%s" + i.split(":")(0))
          System.exit(1)
        }
      }
    }
    val name_list_one_hot = one_hot_feature.split(",")
    if (count_one_hot.toInt != name_list_one_hot.length) {
      println("mismatched, count_one_hot:%d" + count_one_hot + ", name_list_one_hot.length:" + name_list_one_hot.length.toString)
      System.exit(1)
    }
    var importedDf: DataFrame = null

    if (sample_path.startsWith("hdfs://")) {
      importedDf = spark.read.format("tfrecords").option("recordType", "Example").load(sample_path).repartition(3000)
    } else {
      val rdd = spark.sql(generateSql(model_name, update_type, hour, curday, sample_path, pt))
        .rdd.map(x => Base64.decodeBase64(x.getString(0)))
        .filter(_ != null)
      val exampleRdd = rdd.map(x => Example.parseFrom(new BytesWritable(x).getBytes))
      val finalSchema = TensorFlowInferSchema(exampleRdd)
      val rowRdd = exampleRdd.map(example => DefaultTfRecordRowDecoder.decodeExample(example, finalSchema))
      importedDf = spark.createDataFrame(rowRdd, finalSchema).repartition(3000)
    }

    println("file read finish")
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
    one_hot_feature_id_num = one_hot_feature_id_num + "," + sample_count.toString
    //统计每个multi-hot特征的平均id量
    val feature_count = importedDf.select(countIdNumber(count_multi_hot.toInt)($"idx1").alias("sparse"))
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
           |from dl_cpc.cpc_feature_monitor where model_name="$model_name" and day in (
           |select max(day) from dl_cpc.cpc_feature_monitor where model_name="$model_name" and day<"$curday")
           |""".stripMargin)
      if(feature_value.count()==0){
        print("feature exception: Unable to find the historical information of this feature in the model")
        System.exit(1)
      }
      val feature_count = feature_value.rdd.map(r => (r.getAs[String]("one_hot_feature_count"),r.getAs[String]("one_hot_feature_id_num"),r.getAs[String]("multi_hot_feature_count"))).collect()
      val one_hot_feature_count_his = feature_count(0)._1.split(",")
      val one_hot_feature_id_num_his = feature_count(0)._2.split(",")
      val multi_hot_feature_count_his = feature_count(0)._3.split(",")
      val one_hot_feature_count_cur = one_hot_feature_count.split(",")
      val one_hot_feature_id_num_cur = one_hot_feature_id_num.split(",")
      val multi_hot_feature_count_cur = multi_hot_feature_count.split(",")
      for(i <- 0 until one_hot_feature_count_his.length){
        if(threshold1.contains(i.toString)){
          if(one_hot_feature_count_his(i).toFloat/one_hot_feature_count_cur(i).toFloat>threshold1(i.toString).toFloat || one_hot_feature_count_cur(i).toFloat/one_hot_feature_count_his(i).toFloat>threshold1(i.toString)){
            exception_feature = exception_feature :+ name_list_one_hot(i)
          }
        } else {
          if(one_hot_feature_count_his(i).toFloat/one_hot_feature_count_cur(i).toFloat>1.3 || one_hot_feature_count_cur(i).toFloat/one_hot_feature_count_his(i).toFloat>1.3){
            exception_feature = exception_feature :+ name_list_one_hot(i)
          }
        }
      }
      for(i <- 0 until one_hot_feature_id_num_his.length){
        if(threshold2.contains(i.toString)){
          if(one_hot_feature_id_num_cur(i).toFloat/one_hot_feature_id_num_his(i).toFloat>threshold2(i.toString) || one_hot_feature_id_num_his(i).toFloat/one_hot_feature_id_num_cur(i).toFloat>threshold2(i.toString)){
            if(i == count_one_hot.toInt){
              exception_feature = exception_feature :+ "sample_count"
            } else {
              exception_feature = exception_feature :+ name_list_one_hot(i) + "_idCount"
            }
          }
        }
        else {
          if(one_hot_feature_id_num_cur(i).toFloat/one_hot_feature_id_num_his(i).toFloat>1.3 || one_hot_feature_id_num_his(i).toFloat/one_hot_feature_id_num_cur(i).toFloat>1.3){
            if(i == count_one_hot.toInt){
              exception_feature = exception_feature :+ "sample_count"
            } else {
              exception_feature = exception_feature :+ name_list_one_hot(i) + "_idCount"
            }
          }
        }
      }
      for(i <- 0 until multi_hot_feature_count_his.length){
        if(threshold3.contains(i.toString)){
          if(multi_hot_feature_count_cur(i).toFloat/multi_hot_feature_count_his(i).toFloat>threshold3(i.toString) || multi_hot_feature_count_his(i).toFloat/multi_hot_feature_count_cur(i).toFloat>threshold3(i.toString)){
            exception_feature = exception_feature :+ "multihot_" + i.toString
          }
        } else {
          if(multi_hot_feature_count_cur(i).toFloat/multi_hot_feature_count_his(i).toFloat>1.3 || multi_hot_feature_count_his(i).toFloat/multi_hot_feature_count_cur(i).toFloat>1.3){
            exception_feature = exception_feature :+ "multihot_" + i.toString
          }
        }
      }
    }
    if(update_type == "hourly" || update_type == "halfhourly"){
      val feature_value = spark.sql(
        s"""
           |select one_hot_feature_count, one_hot_feature_id_num, multi_hot_feature_count
           |from dl_cpc.cpc_feature_monitor where model_name="$model_name" and hour='$hour' and day in (
           |select max(day) from dl_cpc.cpc_feature_monitor where model_name="$model_name" and day<"$curday" and hour='$hour')
           |""".stripMargin)
      if(feature_value.count()==0){
        print("feature exception: Unable to find the historical information of this feature in the model")
        System.exit(1)
      }
      val feature_count = feature_value.rdd.map(r => (r.getAs[String]("one_hot_feature_count"),r.getAs[String]("one_hot_feature_id_num"),r.getAs[String]("multi_hot_feature_count"))).collect()
      val one_hot_feature_count_his = feature_count(0)._1.split(",")
      val one_hot_feature_id_num_his = feature_count(0)._2.split(",")
      val multi_hot_feature_count_his = feature_count(0)._3.split(",")
      val one_hot_feature_count_cur = one_hot_feature_count.split(",")
      val one_hot_feature_id_num_cur = one_hot_feature_id_num.split(",")
      val multi_hot_feature_count_cur = multi_hot_feature_count.split(",")
      for(i <- 0 until one_hot_feature_count_his.length){
        if(threshold1.contains(i.toString)){
          if(one_hot_feature_count_his(i).toFloat/one_hot_feature_count_cur(i).toFloat>threshold1(i.toString).toFloat || one_hot_feature_count_cur(i).toFloat/one_hot_feature_count_his(i).toFloat>threshold1(i.toString)){
            exception_feature = exception_feature :+ name_list_one_hot(i)
          }
        } else {
          if(one_hot_feature_count_his(i).toFloat/one_hot_feature_count_cur(i).toFloat>1.3 || one_hot_feature_count_cur(i).toFloat/one_hot_feature_count_his(i).toFloat>1.3){
            exception_feature = exception_feature :+ name_list_one_hot(i)
          }
        }
      }
      for(i <- 0 until one_hot_feature_id_num_his.length){
        if(threshold2.contains(i.toString)){
          if(one_hot_feature_id_num_cur(i).toFloat/one_hot_feature_id_num_his(i).toFloat>threshold2(i.toString) || one_hot_feature_id_num_his(i).toFloat/one_hot_feature_id_num_cur(i).toFloat>threshold2(i.toString)){
            if(i == count_one_hot.toInt){
              exception_feature = exception_feature :+ "sample_count"
            } else {
              exception_feature = exception_feature :+ name_list_one_hot(i) + "_idCount"
            }
          }
        }
        else {
          if(one_hot_feature_id_num_cur(i).toFloat/one_hot_feature_id_num_his(i).toFloat>1.3 || one_hot_feature_id_num_his(i).toFloat/one_hot_feature_id_num_cur(i).toFloat>1.3){
            if(i == count_one_hot.toInt){
              exception_feature = exception_feature :+ "sample_count"
            } else {
              exception_feature = exception_feature :+ name_list_one_hot(i) + "_idCount"
            }
          }
        }
      }
      for(i <- 0 until multi_hot_feature_count_his.length){
        if(threshold3.contains(i.toString)){
          if(multi_hot_feature_count_cur(i).toFloat/multi_hot_feature_count_his(i).toFloat>threshold3(i.toString) || multi_hot_feature_count_his(i).toFloat/multi_hot_feature_count_cur(i).toFloat>threshold3(i.toString)){
            exception_feature = exception_feature :+ "multihot_" + i.toString
          }
        } else {
          if(multi_hot_feature_count_cur(i).toFloat/multi_hot_feature_count_his(i).toFloat>1.3 || multi_hot_feature_count_his(i).toFloat/multi_hot_feature_count_cur(i).toFloat>1.3){
            exception_feature = exception_feature :+ "multihot_" + i.toString
          }
        }
      }
    }
    if(exception_feature.length>0){
      print("feature exception" + exception_feature.toString())
      System.exit(1)
    }
  }
}