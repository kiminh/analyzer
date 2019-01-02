package com.cpc.spark.ml.novel

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * dnn ctr v3  小时任务
  * 共28个dense，15个multi hot特征，
  * created time : 2018/11/01 14:34
  *
  * @author zhj
  * @version 1.0
  *
  */

object readtfrecord {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("dnn sample")
        .enableHiveSupport()
        .getOrCreate()

      val sample = spark.read
          .format("tfrecords")
        .option("recordType", "Example")
        .load(s"/user/cpc/wy/dnn_novel_cvr_v1/dnntrain-2018-11-18")
     // sample.take(10).foreach(println)

          sample.filter("sample_idx=3566701")
          .select("idx1","id_arr","dense").show(false)

//    tag.show(5)
//    tag.take(1).foreach(println)


    }
  }

