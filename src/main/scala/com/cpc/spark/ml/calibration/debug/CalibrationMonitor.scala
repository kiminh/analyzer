package com.cpc.spark.ml.calibration.debug

import java.io.{File, FileInputStream, PrintWriter}

import com.cpc.spark.common.Utils
import com.cpc.spark.common.Utils.sendMail
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.{IRModel, PostCalibrations}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, udf, _}
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object CalibrationMonitor {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val modelPath = args(0)
    val dt = args(1)
    val hour = args(2)
    val modelName = args(3)


    println(s"modelPath=$modelPath")
    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"modelName=$modelName")

    val calimap = new PostCalibrations().mergeFrom(CodedInputStream.newInstance(new FileInputStream(modelPath))).caliMap


    val modelset=calimap.keySet
    val session = Utils.buildSparkSession("calibration_check")

    // get union log
    val sql = s"""
                 |select a.isclick,a.exp_ctr as ectr,a.searchid, a.raw_ctr, substring(a.adclass,1,6) as adclass,
                 |a.adslotid as adslot_id, a.ideaid, b.f88[0] as key,b.f89[0] as model_md5,b.f85[0] snapshot_rawctr,
                 |b.f86[0] snapshot_postctr,b.f83[0] snapshot_ectr,b.f61[0] show_count,
                 |case
                 |  when user_req_ad_num in (0,1) then '0'
                 |  when user_req_ad_num = 2 then '1'
                 |  when user_req_ad_num = 3 then '2'
                 |  when user_req_ad_num in (4,5) then '4'
                 |  when user_req_ad_num in (6,7,8) then '7'
                 |  else '8' end as user_req_ad_num
                 |from dl_cpc.slim_union_log a
                 |left join dl_cpc.cpc_ml_nested_snapshot b
                 |on a.searchid = b.searchid and b.day = '$dt' and b.hour = '$hour' and pt = 'qtt'
                 |  where a.dt = '$dt' and a.hour = '$hour'
                 |  and a.ctr_model_name = '$modelName' and a.isshow = 1
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql).withColumn("group1",concat_ws("_",col("adclass"),col("ideaid"),col("user_req_ad_num"),col("adslot_id")))
      .withColumn("group2",concat_ws("_",col("adclass"),col("ideaid"),col("user_req_ad_num")))
      .withColumn("group3",concat_ws("_",col("adclass"),col("ideaid")))
      .withColumn("group4",col("adclass"))
      .withColumn("group",when(searchMap(modelset)(col("group4")),col("group4")).otherwise(lit("0")))
      .withColumn("group",when(searchMap(modelset)(col("group3")),col("group3")).otherwise(col("group")))
      .withColumn("group",when(searchMap(modelset)(col("group2")),col("group2")).otherwise(col("group")))
      .withColumn("group",when(searchMap(modelset)(col("group1")),col("group1")).otherwise(col("group")))
      .withColumn("len",length(col("group")))
      .withColumn("isload",when(col("group")===col("key"),lit(1)).otherwise(0))
      .select("isclick","raw_ctr","ectr","searchid","group","group1","group2","group3","adslot_id","user_req_ad_num"
        ,"show_count","key","model_md5","snapshot_rawctr","snapshot_postctr","snapshot_ectr")

    log.show(50)
    val data_sum = log.count()
    println("total data:%d".format(data_sum))

    val data = log.filter("isload = 1")
    val data_rightkey = data.count()
    println("calibration data:%d".format(data_rightkey))

    if(data_rightkey*1.0/data_sum < 0.95){sendMail("calibration errors", "unmatched key exceeds 5%",
      Seq("wangyao@qutoutiao.net"))}

    data.show(20)
    val result = data.rdd.map( x => {
      val isClick = x.getLong(0).toDouble
      val ectr = x.getLong(1).toDouble / 1e6d
      val onlineCtr = x.getLong(2).toDouble / 1e6d
      val group = x.getString(4)
      val irModel = calimap.get(group).get
      val calibrated = computeCalibration(ectr, irModel.ir.get)
      var mistake = 0
      if (Math.abs(onlineCtr - calibrated) / calibrated > 0.2) {
        mistake = 1
      }
      (isClick, ectr, calibrated, 1.0, onlineCtr, mistake)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
    val ctr = result._1 / result._4
    val ectr = result._2 / result._4
    val calibrated_ctr = result._3 / result._4
    val onlineCtr = result._5 / result._4
    println(s"impression: ${result._4}")
    println(s"mistake: ${result._6}")
    println(s"ctr: $ctr")
    println(s"ectr: $ectr")
    println(s"online ctr: $onlineCtr")
    println(s"calibrated_ctr: $calibrated_ctr")
    println(s"no calibration: ${ectr / ctr}")
    println(s"online calibration: ${onlineCtr / ctr}")
    println(s"new calibration: ${calibrated_ctr / ctr}")

    if(result._6 *1.0 /result._4 > 0.03){sendMail("calibration errors", "mistake exceeds 3%",
      Seq("wangyao@qutoutiao.net"))}
  }

  def searchMap(modelset:Set[String])= udf{
    (key: String)=>{

      var flag = true
      if(modelset contains(key)){
        flag = true
      }
      else{
        flag = false
      }
      flag
    }
  }

  def binarySearch(num: Double, boundaries: Seq[Double]): Int = {
    var index = 0
    var min = 0
    var max = boundaries.size - 1
    while (min <= max) {
      index = (min + max)/ 2
      if (num > boundaries(index)){
        min = index + 1
      }
      else if (num < boundaries(index)) {
        max = index - 1
      }
      else {
        return index
      }
    }
    return min
  }

  def computeCalibration(prob: Double, irModel: IRModel): Double = {
    if (prob <= 0) {
      return 0.0
    }
    var index = binarySearch(prob, irModel.boundaries)
    if (index == 0) {
      return  Math.min(1.0, irModel.predictions(0) * prob/ irModel.boundaries(0))
    }
    if (index == irModel.boundaries.size) {
      index = index - 1
    }
    return Math.max(0.0, Math.min(1.0, irModel.predictions(index-1) +
      (irModel.predictions(index) - irModel.predictions(index-1))
        * (prob - irModel.boundaries(index-1))
        / (irModel.boundaries(index) - irModel.boundaries(index-1))))
  }
}
