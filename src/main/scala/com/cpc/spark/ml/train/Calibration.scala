package com.cpc.spark.ml.train


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.Map

import scala.math.random

object Calibration {

  def ctr_calibration(cali_bin: Array[Double], cali_para: Array[Double], raw_ctr: Double): Double = {
    var cali_ctr: Double = 0.0
    val max: Int = cali_para.length - 1
    if(raw_ctr <= cali_bin(0)){
      cali_ctr = cali_para(0)
    }else if(raw_ctr >= cali_bin(max)){
      cali_ctr = cali_para(max)
    }else{
      val idx: Int = bisect(cali_bin, raw_ctr)
      val upper_bin = cali_bin(idx)
      val lower_bin = cali_bin(idx - 1)
      val upper_para = cali_para(idx)
      val lower_para = cali_para(idx -1 )
      val alpha: Double = (upper_bin - raw_ctr)/(upper_bin - lower_bin)
      cali_ctr = alpha*lower_para + (1.0 - alpha)*upper_para
    }
    return cali_ctr
  }

  def bisect[T](array: Array[T], value: T)(implicit arichmetic: Numeric[T]): Int = {
    var left: Int = 0
    var right: Int = array.length - 1
    var mid: Int = 0
    while (right >= left) {
      mid = left + (right - left) / 2
      val comp = arichmetic.compare(array(mid), value)
      if (comp == 0) {
        return mid
      } else if (comp > 0) {
        right = mid - 1
      } else if (comp < 0) {
        left = mid + 1
      }
    }
    if (mid > right)
      return mid
    if (mid < left)
      return (mid + 1)
    return -1
  }

  def main(args: Array[String]) : Unit = {
    //////////////////////////////////////deal withe the args///////////////////////////////////////
    if(args.length < 6)
    {
      println("Usage: <SparkHome:String> <ModelPath:String> <InputPath:String> <OutputPath:String> <jobDoneFile:String>" )
      System.exit(1);
    }
    var output_arr = new ArrayBuffer[String]()
    val sparkHome = args(0)
    val model_path = args(1)
    output_arr + ("model_path" + "\t" + model_path)
    val cali_data_path = args(2)
    output_arr += ("cali_data_path" + "\t" + cali_data_path)
    val predict_file = args(3)
    output_arr += ("predict_file" + "\t" + predict_file)
    val cali_file = args(4)
    output_arr += ("cali_file" + "\t" + cali_file)
    val done_file = args(5)
    var cali_bin_pv = 50000
    if(args.length == 7 && args(6) != "")
      cali_bin_pv = args(6).toInt
    output_arr += ("cali_bin_pv" + "\t" + cali_bin_pv)
    var random_sample = 0.3
    if(args.length == 8 && args(7) != "")
      random_sample = args(7).toDouble
    output_arr += ("random_sample" + "\t" + random_sample)
    ///////////////////////////////end deal with the args////////////////////////////////////////

    ////////////////////////////////////set up the spark context//////////////////////////
    val conf = new SparkConf()
    conf.setSparkHome(sparkHome)
    conf.set("spark.storage.memoryFraction", "0.7")
    conf.set("spark.speculation", "true")
    conf.set("spark.storage.blockManagerHeartBeatMs", "300000")
    conf.set("spark.scheduler.maxRegisteredResourcesWaitingTime", "100")
    conf.set("spark.core.connection.auth.wait.timeout", "100")
    conf.set("spark.akka.frameSize", "100")
    val sc = new SparkContext(conf)
    /////////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////load the lr model/////////////////////////////////
    val model_lines = sc.textFile(model_path, 1)
    val paras = model_lines.map {line=>
      val line_arr = line.split("\t")
      (line_arr(0), line_arr(1).toDouble)
    }.collectAsMap
    val paras_b = sc.broadcast(paras)
    ////////////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////predict the cali data/////////////////////////////
    val cali_data = sc.textFile(cali_data_path, 30)
    var predict_res_rdd = cali_data.map { line =>
      var insProb = 0.0
      var is_clk = 0
      if (scala.math.random < random_sample) {
        val fields = line.split("\t")
        is_clk = fields(1).toInt
        var score = 0.0
        fields.filter(_.length() > 5).foreach(f =>
          if (paras_b.value.contains(f)) {
            score += paras_b.value(f)
          }
        )
        if (score > 30) {
          insProb = 1.0
        } else if (score < -30) {
          insProb = 0.0
        } else {
          insProb = 1.0 / (1.0 + Math.exp(-1.0 * score))
        }

      }
      (f"$insProb%.5f", (is_clk, 1))
    }.combineByKey(
      (x: (Int, Int)) => (x._1, x._2),
      (acc:(Int, Int), x) => (acc._1 + x._1, acc._2 + x._2),
      (acc1:(Int, Int), acc2:(Int, Int)) => (acc1._1+acc2._1, acc1._2 + acc2._2)
    ).sortByKey(false)
    //.reduceByKey{(a,b)=>(a._1+b._1, a._2+b._2)}.sortByKey(false)
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////// gen cali para /////////////////////////////////////////////
    var total_pv = 0.0
    var total_clk = 0.0
    val total_predict_res = predict_res_rdd.collect()
    total_predict_res.foreach{x => total_pv += x._2._2; total_clk += x._2._1;}
    output_arr += ("total_pv" + "\t" + total_pv)
    output_arr += ("total_clk" + "\t" + total_clk)
    var cali_array = new ArrayBuffer[String]()
    var current_pv = 1.0
    var current_clk = 0.0
    var cali_last_ctr = 1.0
    val pos_cnt = total_clk
    val neg_cnt = total_pv - total_clk
    var pos_gt_neg_cnt = 0.0
    var rank_value = total_pv
    var auc_tmp = 0.0
    var TP = 0.0
    var TP_pre = 0.0
    var FP = 0.0
    var FP_pre = 0.0
    total_predict_res.foreach{x =>
      if(current_pv>=cali_bin_pv)
      {
        val observed_ctr = (current_clk+0.0)/(current_pv+0.0)
        cali_array += (cali_last_ctr + "\t" + f"$observed_ctr%.7f")
        current_pv = 1.0
        current_clk = 0.0
        cali_last_ctr = x._1.toDouble
      }
      //cali_last_ctr = x._1.toDouble
      current_pv += x._2._2
      current_clk += x._2._1
      //cal auc
      auc_tmp += ((TP+TP_pre)*(FP-FP_pre))/2.0
      TP_pre = TP
      FP_pre = FP
      TP = TP + x._2._1
      FP = FP + (x._2._2-x._2._1)
    }
    auc_tmp += ((TP+TP_pre)*(FP-FP_pre))/2.0
    val auc = auc_tmp / (TP*FP)
    output_arr += ("auc" + "\t" + auc)
    //最后一段
    if (current_pv>1000){ //对于最后一段pv太小的直接抛弃
      val observed_ctr = (current_clk+0.1)/(current_pv+1000.0)
      cali_array += (cali_last_ctr + "\t" + f"$observed_ctr%.7f")
    }
    ///////////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////cal the OE///////////////////////////////////////
    var cali_bin = new ArrayBuffer[Double]()
    var cali_para = new ArrayBuffer[Double]()
    cali_array.foreach{ x=>
      var line_arr = x.split("\t")
      cali_bin += line_arr(0).toDouble
      cali_para += line_arr(1).toDouble
    }
    val cali_bin_arr = cali_bin.reverse.toArray
    val cali_para_arr = cali_para.reverse.toArray
    var observed_clk:Double = 0.0
    var expected_clk:Double = 0.0
    total_predict_res.foreach{x=>
      val raw_ctr: Double = x._1.toDouble
      val pv: Double = x._2._2.toDouble
      val clk: Double = x._2._1.toDouble
      val cali_ctr: Double = ctr_calibration(cali_bin_arr, cali_para_arr, raw_ctr)
      expected_clk += pv * cali_ctr
      observed_clk += clk
    }
    val oe: Double = observed_clk/expected_clk
    output_arr += ("observed_clk" + "\t" + observed_clk)
    output_arr += ("expected_clk" + "\t" + expected_clk)
    output_arr += ("oe" + "\t" + oe)
    //////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////write result///////////////////////////////////////////
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.13.121:10010"), hadoopConf)
    try {hdfs.delete(new org.apache.hadoop.fs.Path(cali_file), true)} catch{case _: Throwable => {}}
    val cali_rdd = sc.makeRDD(cali_array, 1)
    cali_rdd.saveAsTextFile(cali_file)
    try {hdfs.delete(new org.apache.hadoop.fs.Path(predict_file), true)} catch{case _: Throwable => {}}
    predict_res_rdd.saveAsTextFile(predict_file)
    try {hdfs.delete(new org.apache.hadoop.fs.Path(done_file), true)} catch{case _: Throwable => {}}
    val done_rdd = sc.makeRDD(output_arr, 1)
    done_rdd.saveAsTextFile(done_file)
    ///////////////////////////////////////////////////////////////////////////////////
  }
}

