package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}

import com.cpc.spark.common.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.ml.calibration.CalibrationCheckOnMidu.searchMap
import com.cpc.spark.ml.calibration.exp.LrCalibrationOnQtt.calculateAuc

object CvrCaliTest{

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
  val newDestDir = "/home/cpc/model_server/calibration/"
  val MAX_BIN_COUNT = 10
  val MIN_BIN_SIZE = 100000

  def main(args: Array[String]): Unit = {

    // build spark session
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val calimodel = "qtt-cvr-dnn-rawid-v1-180"
    // get union log
    val sql = s"""
                 |select iscvr as isclick, cast(raw_cvr as bigint) as ectr, cvr_model_name, adslotid, cast(ideaid as string) ideaid,
                 |case when user_req_ad_num = 1 then '1'
                 |  when user_req_ad_num = 2 then '2'
                 |  when user_req_ad_num in (3,4) then '4'
                 |  when user_req_ad_num in (5,6,7) then '7'
                 |  else '8' end as user_req_ad_num
                 |  from dl_cpc.qtt_cvr_calibration_sample where dt = '2019-05-20'
       """.stripMargin
    println(s"sql:\n$sql")
    val log = spark.sql(sql)

    println("datasum: %d".format(log.count()))
    println("datasum: %d".format(log.filter("iscvr = 1").count()))

    val group1 = log.groupBy("ideaid","user_req_ad_num","adslotid").count().withColumn("count1",col("count"))
      .withColumn("group",concat_ws("_",col("ideaid"),col("user_req_ad_num"),col("adslotid")))
      .filter("count1>100000")
      .select("ideaid","user_req_ad_num","adslotid","group")
    val group2 = log.groupBy("ideaid","user_req_ad_num").count().withColumn("count2",col("count"))
      .withColumn("group",concat_ws("_",col("ideaid"),col("user_req_ad_num")))
      .filter("count2>100000")
      .select("ideaid","user_req_ad_num","group")
    val group3 = log.groupBy("ideaid").count().withColumn("count3",col("count"))
      .filter("count3>10000")
      .withColumn("group",col("ideaid"))
      .select("ideaid","group")

    val data1 = log.join(group1,Seq("user_req_ad_num","adslotid","ideaid"),"inner")
    val data2 = log.join(group2,Seq("ideaid","user_req_ad_num"),"inner")
    val data3 = log.join(group3,Seq("ideaid"),"inner")

    //create cali pb
    val calimap1 = GroupToConfig(data1, spark,calimodel)
    val calimap2 = GroupToConfig(data2, spark,calimodel)
    val calimap3 = GroupToConfig(data3, spark,calimodel)
    val calimap4 = GroupToConfig(log.withColumn("group",lit("0")), spark,calimodel)
    val calimap = (calimap1 ++ calimap2 ++ calimap3 ++ calimap4).toMap

    val localPath ="/home/cpc/wy/qtt_v180_k_cali.txt"
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    new PrintWriter(localPath) { write(calimap.toString); close() }

    val modelset=calimap.keySet
    val sql2 = s"""
                 |select iscvr as isclick,searchid, raw_cvr as raw_ctr, cast(exp_cvr as bigint) as ectr, cvr_model_name, adslotid, cast(ideaid as string) ideaid,
                 |case when user_req_ad_num = 1 then '1'
                 |  when user_req_ad_num = 2 then '2'
                 |  when user_req_ad_num in (3,4) then '4'
                 |  when user_req_ad_num in (5,6,7) then '7'
                 |  else '8' end as user_req_ad_num
                 |  from dl_cpc.qtt_cvr_calibration_sample where dt = '2019-05-20'
       """.stripMargin
    val test = spark.sql(sql2)
      .withColumn("group1",concat_ws("_",col("cvr_model_name"),col("ideaid"),col("user_req_ad_num"),col("adslotid")))
      .withColumn("group2",concat_ws("_",col("cvr_model_name"),col("ideaid"),col("user_req_ad_num")))
      .withColumn("group3",concat_ws("_",col("cvr_model_name"),col("ideaid")))
      .withColumn("group",when(searchMap(modelset)(col("group3")),col("group3")).otherwise(concat_ws("_",col("cvr_model_name"),lit("0"))))
      .withColumn("group",when(searchMap(modelset)(col("group2")),col("group2")).otherwise(col("group")))
      .withColumn("group",when(searchMap(modelset)(col("group1")),col("group1")).otherwise(col("group")))
      .withColumn("len",length(col("group")))
      .select("isclick","raw_ctr","ectr","searchid","group","group1","group2","group3","cvr_model_name","adslotid","ideaid","user_req_ad_num","len")


    val result = test.rdd.map( x => {
      val isClick = x.getInt(0).toDouble
      val ectr = x.getLong(1).toDouble / 1e6d
      val onlineCtr = x.getLong(2).toDouble / 1e6d
      val group = x.getString(4)
      val k = calimap.get(group).get
      val calibrated = k*ectr
      var mistake = 0
      if (Math.abs(onlineCtr - calibrated) / calibrated > 0.2) {
        mistake = 1
      }
      (isClick, ectr, calibrated, 1.0, onlineCtr)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))
    val ctr = result._1 / result._4
    val ectr = result._2 / result._4
    val calibrated_ctr = result._3 / result._4
    val onlineCtr = result._5 / result._4
    println(s"impression: ${result._4}")
    println(s"ctr: $ctr")
    println(s"ectr: $ectr")
    println(s"online ctr: $onlineCtr")
    println(s"calibrated_ctr: $calibrated_ctr")
    println(s"no calibration: ${ectr / ctr}")
    println(s"online calibration: ${onlineCtr / ctr}")
    println(s"new calibration: ${calibrated_ctr / ctr}")

    val result2 = test.rdd.map(
      x =>{
        val isClick = x.getInt(0).toDouble
        val ectr = x.getLong(1).toDouble / 1e6d
        val onlineCtr = x.getLong(2).toDouble / 1e6d
        val group = x.getString(4)
        val k = calimap.get(group).get
        val calibrated = k*ectr
        val ideaid = x.getString(10)
        (ectr,calibrated,ideaid, isClick)
      }).toDF("ectr","calibrated","ideaid","isclick")
      .selectExpr("cast(isclick as Int) label","cast(calibrated* 1e6d as Int) prediction","ideaid")

    calculateAuc(result2,"k value",spark)
  }


  def GroupToConfig(data:DataFrame, session: SparkSession, calimodel: String, minBinSize: Int = MIN_BIN_SIZE,
                    maxBinCount : Int = MAX_BIN_COUNT, minBinCount: Int = 1): scala.collection.mutable.Map[String,Double] = {
    val sc = session.sparkContext
    import session.implicits._
    var calimap = scala.collection.mutable.Map[String,Double]()
    val result = data.select("user_req_ad_num","adslotid","ideaid","isclick","ectr","cvr_model_name","group")
      .rdd.map( x => {
      var isClick = 0d
      if (x.get(3) != null) {
        isClick = x.getInt(3).toDouble
      }
      val ectr = x.getLong(4).toDouble / 1e6d
      val model = x.getString(5)
      val group = x.getString(6)
      val key = calimodel + "_" + group
      (key, ectr, isClick)
    }).toDF("key","ectr","isclick")
      .groupBy("key")
      .agg(avg(col("ectr")).alias("avg_ectr"),
        sum(col("isclick")).alias("ctrnum"),
        count(col("ectr")).alias("show")
      )
      .withColumn("ctr",col("ctrnum")/col("show"))
      .withColumn("kvalue",col("ctr")/col("avg_ectr"))
      .rdd.collect().foreach {
        x =>{
          val modelName = x.getAs[String]("key")
          val show = x.getAs[Long]("show")
          val ctrnum = x.getAs[Double]("ctrnum")
          val kvalue = x.getAs[Double]("kvalue")
          println(s"model: $modelName has data of size $show, of positive number of $ctrnum")
          calimap.update(modelName, kvalue)
        }
        calimap
      }
    return calimap
  }

  // input: (<ectr, click>)
  // output: original ectr/ctr, calibrated ectr/ctr

  def binarySearch(num: Double, boundaries: Seq[(Double,Double)]): Int = {
    if (num < boundaries(0)._1) {
      return 0
    }
    if (num >= boundaries.last._1) {
      val a = boundaries.size
      return boundaries.size
    }
    val mid = boundaries.size / 2
    if (num < boundaries(mid)._1) {
      return binarySearch(num, boundaries.slice(0, mid))
    } else {
      return binarySearch(num, boundaries.slice(mid, boundaries.size)) + mid
    }
  }

  def computeCalibration(prob: Double, model: Seq[(Double, Double)]): Double = {

    if (prob <= 0) {
      return 0.0
    }
    var index = binarySearch(prob, model)
    if (index == 0) {
      return  Math.min(1.0, model(0)._2 * prob)
    }
    if (index == model.size) {
      index = index - 1
    }
    return Math.max(0.0, Math.min(1.0, model(index)._2 * prob))
  }

  // input: Seq<(<ectr, click>)
  // return: (Seq(<ctr, ectr, weight>), total count)
  def binIterable(data: Iterable[(Double, Double)], minBinSize: Int, maxBinCount: Int)
  : (Seq[(Double, Double)], Double, Double) = {
    val dataList = data.toList
    val totalSize = dataList.size
    val binNumber = Math.min(Math.max(1, totalSize / minBinSize), maxBinCount)
    val binSize = totalSize / binNumber
    var bins = Seq[(Double, Double)]()
    var allClickSum = 0d
    var clickSum = 0d
    var showSum = 0d
    var eCtrSum = 0d
    var n = 0
    dataList.sorted.foreach {
      x =>
        var ectr = 0.0
        if (x._1 > 0) {
          ectr = x._1
        }
        eCtrSum = eCtrSum + ectr
        if (x._2 > 1e-6) {
          clickSum = clickSum + 1
          allClickSum = allClickSum + 1
        }
        showSum = showSum + 1
        if (showSum >= binSize) {
          val ctr = clickSum / showSum
          bins = bins :+((ectr, ctr * showSum / eCtrSum))
          n = n + 1
          clickSum = 0d
          showSum = 0d
          eCtrSum = 0d
        }
    }
    return (bins, totalSize, allClickSum)
  }

}
