package com.cpc.spark.ml.ctrmodel.hourly

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, WrappedArray}
import scala.util.Random

/**
  * Created by zhaolei on 22/12/2017.
  * new owner: fym (190511).
  */
object LRTrain {

  private var trainLog = Seq[String]()
  private val model = new LRIRModel

  private var dict = mutable.Map[String, Map[Int, Int]]()

  val dictNames = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid"
  )

  private var dictStr: mutable.Map[String, Map[String, Int]] = mutable.Map[String, Map[String, Int]]()
  private var dictLong: mutable.Map[String, Map[Long, Int]] = mutable.Map[String, Map[Long, Int]]()

  val dictIntNamesForV6: Seq[String] = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid",
    "userid",
    "siteid",
    "doc_cat"
  )

  val dictStrNamesForV6: Seq[String] = Seq(
    "channel",
    "dtu_id",
    "brand"
  )

  val dictLongNamesForV6: Seq[String] = Seq(
    "doc_id"
  )

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark: SparkSession = model
      .initSpark("[cpc-model] qtt-bs-ctrparser6-daily")

    // 按分区取数据
    val ctrPathSep = getPathSeq(args(0).toInt)
    val cvrPathSep = getPathSeq(args(1).toInt)

    val date = args(2)
    val hour = args(3)

    // initFeatureDict(spark, ctrPathSep)

    initIntFeatureDictV6(spark, ctrPathSep)
    initLongFeatureDictV6(spark, ctrPathSep)
    initStringFeatureDictV6(spark, ctrPathSep)

    val userAppIdx = getUidApp(spark, ctrPathSep).cache()

    // fym 190512: to replace getData().
    val queryRawDataFromUnionEvents =
      s"""
         |select
         |  searchid
         |  , sex
         |  , age
         |  , os
         |  , network
         |  , isp
         |  , city
         |  , media_appsid
         |  , phone_level
         |  , `timestamp`
         |  , adtype
         |  , planid
         |  , unitid
         |  , ideaid
         |  , adclass
         |  , adslot_id as adslotid
         |  , adslot_type
         |  , brand_title as brand
         |  , media_type
         |  , channel
         |  , client_type as sdk_type
         |  , dtu_id
         |  , interaction
         |  , userid
         |  , siteid
         |  , province
         |  , city_level
         |  , content_id as doc_id
         |  , category as doc_cat
         |  , is_new_ad
         |  , uid
         |  , isclick as label
         |  , day
         |from dl_cpc.cpc_basedata_union_events
         |where %s
         |  and isshow = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and adslot_type in (1, 2)
         |  and ideaid > 0
         |  and unitid > 0
       """.stripMargin
        .format(getSelectedHoursBefore(date, hour, 72))

    val rawDataFromTrident = spark
      .sql(queryRawDataFromUnionEvents)

    model.clearResult()

    val qttAll = rawDataFromTrident
      .randomSplit(
        Array(0.1, 0.9), // 3G vs. 40M
        new Date().getTime // seed
      )(0)

    // println(qttAll.count())

    train(
      spark,
      "ctrparser6",
      "qtt-bs-ctrparser6-daily",
      getLeftJoinData(qttAll, userAppIdx),
      "qtt-bs-ctrparser6-daily.lrm",
      date,
      4e8
    )

    Utils
      .sendMail(
        trainLog.mkString("\n"),
        "[cpc-bs-q] qtt-bs-ctrparser6-daily 训练复盘",
        Seq(
          "fanyiming@qutoutiao.net"/*,
          "dongwei@qutoutiao.net",
          "tankaide@qutoutiao.net",
          "wangshixin@qutoutiao.net",
          "chuquanquan@qutoutiao.net",
          "huangweidong@qutoutiao.net"*/
        )
      )

    rawDataFromTrident.unpersist()
    userAppIdx.unpersist()
  }

  def getPathSeq(days: Int): mutable.Map[String, Seq[String]] = {
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -(days * 24 + 2))
    val pathSep = mutable.Map[String, Seq[String]]()

    for (n <- 1 to days * 24) {
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep.update(date, (pathSep.getOrElse(date, Seq[String]()) :+ hour))
      cal.add(Calendar.HOUR, 1)
    }

    pathSep
  }

  def getUidApp(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): DataFrame = {
    val inpath = "hdfs://emr-cluster/user/cpc/userInstalledApp/{%s}".format(pathSep.keys.mkString(","))
    println(inpath)

    import spark.implicits._

    val uidApp = spark
      .read
      .parquet(inpath)
      .rdd
      .map(x =>
        (
          x.getAs[String]("uid"),
          x.getAs[WrappedArray[String]]("pkgs")
        )
      )
      .reduceByKey(_ ++ _)
      .map(x =>
        (
          x._1,
          x._2.distinct
        )
      )
      .toDF("uid", "pkgs")
      .rdd
      .cache()

    val ids = getTopApp(uidApp, 1000)
    dictStr.update("appid", ids)

    val userAppIdx = getUserAppIdx(spark, uidApp, ids)
      .repartition(1000)

    uidApp.unpersist()

    userAppIdx
  }

  //安装列表中top k的App
  def getTopApp(uidApp: RDD[Row], k: Int): Map[String, Int] = {
    var idx = 0
    val ids = mutable.Map[String, Int]()

    uidApp
      .flatMap(x =>
        x.getAs[WrappedArray[String]]("pkgs")
          .map(
            (_, 1)
          )
      )
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .toLocalIterator
      .take(k)
      .foreach {
        id =>
          idx += 1
          ids.update(id._1, idx)
      }
    ids.toMap
  }

  //用户安装列表对应的App idx
  def getUserAppIdx(spark: SparkSession, uidApp: RDD[Row], ids: Map[String, Int]): DataFrame = {
    import spark.implicits._
    uidApp.map {
      x =>
        val k = x.getAs[String]("uid")
        val v = x.getAs[WrappedArray[String]]("pkgs").map(p => (ids.getOrElse(p, 0))).filter(_ > 0)
        (k, v)
    }.toDF("uid", "appIdx")
  }

  // 用户安装列表特征合并到原有特征
  def getLeftJoinData(data: DataFrame, userAppIdx: DataFrame): DataFrame = {
    data.join(userAppIdx, Seq("uid"), "left_outer")
  }

  def train(
             spark: SparkSession,
             parser: String,
             name: String,
             df: DataFrame,
             destfile: String,
             dateForTesting: String,
             numDataToFeed: Double
           ): Unit = {

    trainLog :+= "\n------train log--------"
    trainLog :+= "name = %s".format(name)
    trainLog :+= "parser = %s".format(parser)
    trainLog :+= "destfile = %s".format(destfile)

    val num = df.count().toDouble
    println("sample num", num)
    trainLog :+= "total size %.0f".format(num)

    // 最多n条训练数据
    var trainRate = 0.9
    if (num * trainRate > numDataToFeed) {
      trainRate = numDataToFeed / num
    }

    val train = df
      .filter(x => x.getAs[String]("day") != dateForTesting)

    val test = df
      .filter(x => x.getAs[String]("day") == dateForTesting)

    df.unpersist()

    val tnum = train.count().toDouble
    val pnum = train.filter(_.getAs[Int]("label") > 0).count().toDouble
    val nnum = tnum - pnum

    // 保证训练数据正负比例 1:9
    // fym 190521: no need for IR but we kept prompting pnum/nnum/rate.
    val rate = (pnum * 9 / nnum * 1000).toInt
    println("total positive negative", tnum, pnum, nnum, rate)
    trainLog :+= "train size total=%.0f positive=%.0f negative=%.0f scaleRate=%d/1000".format(tnum, pnum, nnum, rate)

    val sampleTrain = formatSample(spark, parser, train)/*.filter(x => x.getAs[Int]("label") > 0))*/
    val sampleTest = formatSample(spark, parser, test)

    println(sampleTrain.take(5).foreach(x => println(x.features)))
    model.run(sampleTrain, 200, 1e-8)
    model.test(sampleTest)

    model.printLrTestLog()
    trainLog :+= model.getLrTestLog()

    /*val testNum = sampleTest.count().toDouble * 0.9
    val minBinSize = 1000d
    var binNum = 100d
    if (testNum < minBinSize * binNum) {
      binNum = testNum / minBinSize
    }

    model.runIr(binNum.toInt, 0.95)
    trainLog :+= model.binsLog.mkString("\n")*/

    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    val lrfilepathBackup = "/home/cpc/anal/model/lrmodel-%s-%s.lrm".format(name, date)
    val lrFilePathToGo = "/home/cpc/anal/model/togo/%s.lrm".format(name)
//    val mlfilepath = "/home/cpc/anal/model/lrmodel-%s-%s.mlm".format(name, date)
//    val mlfilepathToGo = "/home/cpc/anal/model/togo/%s.mlm".format(name)

    // backup on hdfs.
    model.saveHdfs("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata/%s".format(date))
    model.saveIrHdfs("hdfs://emr-cluster/user/cpc/lrmodel/irmodeldata/%s".format(date))

    // backup on local machine.
    model.savePbPack(parser, lrfilepathBackup, dict.toMap, dictStr.toMap)
//    model.savePbPack2(parser, mlfilepath, dict.toMap, dictStr.toMap)

    // for go-live.
    model.savePbPack(parser, lrFilePathToGo, dict.toMap, dictStr.toMap)
//    model.savePbPack2(parser, mlfilepathToGo, dict.toMap, dictStr.toMap)

    // update trainLog.
    trainLog :+= "protobuf pack (lr-backup) : %s".format(lrfilepathBackup)
    trainLog :+= "protobuf pack (lr-to-go) : %s".format(lrFilePathToGo)
//    trainLog :+= "protobuf pack (ir-backup) : %s".format(mlfilepath)
//    trainLog :+= "protobuf pack (ir-to-go) : %s".format(mlfilepathToGo)
  }

  def formatSample(spark: SparkSession, parser: String, ulog: DataFrame): RDD[LabeledPoint] = {
    val BcDict = spark.sparkContext.broadcast(dict)
    val BcDictStr = spark.sparkContext.broadcast(dictStr)
    val BcDictLong = spark.sparkContext.broadcast(dictLong)

    ulog.rdd
      .mapPartitions {
        p =>
          dict = BcDict.value
          dictStr = BcDictStr.value
          dictLong = BcDictLong.value
          
          p.map {
            u =>
              val vec = parser match {
                case "ctrparser4" =>
                  getCtrVectorParser4(u)
                case "ctrparser5" =>
                  getCtrVectorParser5(u)
                case "ctrparser6" =>
                  getCtrVectorParser6(u)
              }
              LabeledPoint(u.getAs[Int]("label").toDouble, vec)
          }
      }
  }

  def initFeatureDict(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): Unit = {

    trainLog :+= "\n------dict size------"
    for (name <- dictNames) {
      val pathTpl = "hdfs://emr-cluster/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      var n = 0
      val ids = mutable.Map[Int, Int]()
      println(pathTpl.format(name, pathSep.keys.mkString(",")))
      spark.read
        .parquet(pathTpl.format(name, pathSep.keys.mkString(",")))
        .rdd
        .map(x => x.getInt(0))
        .distinct()
        .sortBy(x => x)
        .toLocalIterator
        .foreach {
          id =>
            n += 1
            ids.update(id, n)
        }
      dict.update(name, ids.toMap)
      println("dict", name, ids.size)
      trainLog :+= "%s=%d".format(name, ids.size)
    }
  }

  def getData(spark: SparkSession, dataVersion: String, pathSep: mutable.Map[String, Seq[String]]): DataFrame = {
    trainLog :+= "\n-------get ulog data------"

    var path = Seq[String]()
    pathSep.map {
      x =>
        path = path :+ "/user/cpc/lrmodel/%s/%s/{%s}".format(dataVersion, x._1, x._2.mkString(","))
    }

    path.foreach {
      x =>
        trainLog :+= x
        println(x)
    }

    spark.read.parquet(path: _*).coalesce(600)
  }

  def getCtrVectorParser4(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    //sex
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 9

    //age
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //isp
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //net
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //ad class
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 10

    //adslot_type
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //unitid
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getCtrVectorParser5(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    //sex
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 9

    //age
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //isp
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //net
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //pagenum
    var pnum = x.getAs[Int]("pagenum")
    if (pnum < 0 || pnum > 50) {
      pnum = 0
    }
    els = els :+ (pnum + i, 1d)
    i += 100

    //bookid
    var bid = 0
    try {
      bid = x.getAs[String]("bookid").toInt
    } catch {
      case e: Exception =>
    }
    if (bid < 0 || bid > 50) {
      bid = 0
    }
    els = els :+ (bid + i, 1d)
    i += 100

    //ad class
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 10

    //adslot_type
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //unitid
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //user_req_ad_num
    /*var uran_idx = 0
    val uran = x.getAs[Int]("user_req_ad_num")
    if (uran >= 1 && uran <= 10) {
      uran_idx = uran
    }
    if (uran > 10) {
      uran_idx = 11
    }
    els = els :+ (uran_idx + i, 1d)
    i += 12 + 1

    //user_req_num
    var urn_idx = 0
    val urn = x.getAs[Int]("user_req_num")
    if (urn >= 1 && urn <= 10) {
      urn_idx = 1
    } else if (urn > 10 && urn <= 100) {
      urn_idx = 2
    } else if (urn > 100 && urn <= 1000) {
      urn_idx = 3
    } else if (urn > 1000) {
      urn_idx = 4
    }
    els = els :+ (urn_idx + i, 1d)
    i += 5 + 1

    var user_click = x.getAs[Int]("user_click_num")
    if (user_click >= 5) {
      user_click = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    var user_click_unit = x.getAs[Int]("user_click_unit_num")
    if (user_click_unit >= 5) {
      user_click_unit = 5
    }
    els = els :+ (user_click + i, 1d)
    i += 6

    //sex - age
    if (x.getAs[Int]("sex") > 0 && x.getAs[Int]("age") > 0) {
      els = els :+ (6 * (x.getAs[Int]("sex") - 1) + x.getAs[Int]("age") + i, 1d)
    }
    i += 2 * 6 + 1*/

    //user installed app
    val appIdx = x.getAs[WrappedArray[Int]]("appIdx")
    if (appIdx != null) {
      val inxList = appIdx.map(p => (p + i, 1d))
      els = els ++ inxList
    }
    i += 1000 + 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getCtrVectorParser6(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    //sex
    els = els :+ (x.getAs[Int]("sex") + i, 1d)
    i += 9

    //age
    els = els :+ (x.getAs[Int]("age") + i, 1d)
    i += 100

    //os 96 - 97 (2)
    els = els :+ (x.getAs[Int]("os") + i, 1d)
    i += 10

    //isp
    els = els :+ (x.getAs[Int]("isp") + i, 1d)
    i += 20

    //net
    els = els :+ (x.getAs[Int]("network") + i, 1d)
    i += 10

    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //media id
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //ad slot id
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //0 to 4
    els = els :+ (x.getAs[Int]("phone_level") + i, 1d)
    i += 10

    //ad class
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 10

    //adslot_type
    els = els :+ (x.getAs[Int]("adslot_type") + i, 1d)
    i += 10

    //planid
    els = els :+ (dict("planid").getOrElse(x.getAs[Int]("planid"), 0) + i, 1d)
    i += dict("planid").size + 1

    //unitid
    els = els :+ (dict("unitid").getOrElse(x.getAs[Int]("unitid"), 0) + i, 1d)
    i += dict("unitid").size + 1

    //ideaid
    els = els :+ (dict("ideaid").getOrElse(x.getAs[Int]("ideaid"), 0) + i, 1d)
    i += dict("ideaid").size + 1

    //user installed app
    val appIdx = x.getAs[WrappedArray[Int]]("appIdx")
    if (appIdx != null) {
      val inxList = appIdx.map(p => (p + i, 1d))
      els = els ++ inxList
    }
    i += 1000 + 1

    // 190523: dnn features.

    // brand
    els = els :+ (dictStr("brand").getOrElse(x.getAs[String]("brand"), 0) + i, 1d)
    i += dictStr("brand").size + 1

    // channel
    els = els :+ (dictStr("channel").getOrElse(x.getAs[String]("channel"), 0) + i, 1d)
    i += dictStr("channel").size + 1

    // city_level
    els = els :+ (x.getAs[Int]("city_level") + i - 1, 1d)
    i += 6

    // doc_cat
    els = els :+ (dict("doc_cat").getOrElse(x.getAs[Int]("doc_cat"), 0) + i, 1d)
    i += dict("doc_cat").size + 1

    // doc_cat
    els = els :+ (dictLong("doc_id").getOrElse(x.getAs[Long]("doc_id"), 0) + i, 1d)
    i += dictLong("doc_id").size + 1

    // dtu_id
    els = els :+ (dictStr("dtu_id").getOrElse(x.getAs[String]("dtu_id"), 0) + i, 1d)
    i += dictStr("dtu_id").size + 1

    // interaction
    els = els :+ (x.getAs[Int]("interaction") + i, 1d)
    i += 7

    // is_new_ad
    els = els :+ (x.getAs[Int]("is_new_ad") + i, 1d)
    i += 2

    // media_type
    els = els :+ (x.getAs[Int]("media_type") + i, 1d)
    i += 4

    // province
    els = els :+ (x.getAs[Int]("province") + i, 1d)
    i += 35

    // sdk_type
    val sdk_type_to_int : Int = x.getAs[String]("sdk_type") match {
      case "QTT" =>
        1
      case "HZ" =>
        2
      case "JSSDK" =>
        3
      case "OPENAPI" =>
        4
      case "NATIVESDK" =>
        5
      case "FUN" =>
        6
      case _ => 0
    }

    els = els :+ (sdk_type_to_int + i, 1d)
    i += 7

    // userid
    els = els :+ (dict("userid").getOrElse(x.getAs[Int]("userid"), 0) + i, 1d)
    i += dict("userid").size + 1

    // siteid
    els = els :+ (dict("siteid").getOrElse(x.getAs[Int]("siteid"), 0) + i, 1d)
    i += dict("siteid").size + 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getLimitedData(spark: SparkSession, limitedNum: Double, ulog: DataFrame): DataFrame = {
    import spark.implicits._
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
  }

  def printXGBTestLog(lrTestResults: RDD[(Double, Double)]): Unit = {
    val testSum = lrTestResults.count()
    if (testSum < 0) {
      throw new Exception("must run lr test first or test results is empty")
    }
    var test0 = 0 //反例数
    var test1 = 0 //正例数
    lrTestResults
      .map {
        x =>
          var label = 0
          if (x._2 > 0.01) {
            label = 1
          }
          (label, 1)
      }
      .reduceByKey((x, y) => x + y)
      .toLocalIterator
      .foreach {
        x =>
          if (x._1 == 1) {
            test1 = x._2
          } else {
            test0 = x._2
          }
      }

    var log = "predict distribution %s %d(1) %d(0)\n".format(testSum, test1, test0)
    lrTestResults //(p, label)
      .map {
      x =>
        val v = (x._1 * 100).toInt / 5
        ((v, x._2.toInt), 1)
    } //  ((预测值,lable),1)
      .reduceByKey((x, y) => x + y) //  ((预测值,lable),num)
      .map {
      x =>
        val key = x._1._1
        val label = x._1._2
        if (label == 0) {
          (key, (x._2, 0)) //  (预测值,(num1,0))
        } else {
          (key, (0, x._2)) //  (预测值,(0,num2))
        }
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //  (预测值,(num1反,num2正))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          val pre = x._1.toDouble * 0.05
          if (pre > 0.2) {
            //isUpdateModel = true
          }
          log = log + "%.2f %d %.4f %.4f %d %.4f %.4f %.4f\n".format(
            pre, //预测值
            sum._2, //正例数
            sum._2.toDouble / test1.toDouble, //该准确率下的正例数/总正例数
            sum._2.toDouble / testSum.toDouble, //该准确率下的正例数/总数
            sum._1, //反例数
            sum._1.toDouble / test0.toDouble, //该准确率下的反例数/总反例数
            sum._1.toDouble / testSum.toDouble, //该准确率下的反例数/总数
            sum._2.toDouble / (sum._1 + sum._2).toDouble) // 真实值
      }

    println(log)
    trainLog :+= log
  }

  // fym 190428.
  def getSelectedHoursBefore(
                              date: String,
                              hour: String,
                              hours: Int
                            ): String = {
    val dateHourList = ListBuffer[String]()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    for (t <- 0 until hours-1) {
      if (t > 0) {
        cal.add(Calendar.HOUR, -1)
      }
      val formatDate = dateFormat.format(cal.getTime)
      val datee = formatDate.substring(0, 10)
      val hourr = formatDate.substring(11, 13)

      val dateL = s"(`day`='$datee' and `hour`='$hourr')"
      dateHourList += dateL
    }

    "(" + dateHourList.mkString(" or ") + ")"
  }

  def initIntFeatureDictV6(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): Unit = {

    trainLog :+= "\n------dict size------"
    for (name <- dictIntNamesForV6) {
      val pathTpl = "hdfs://emr-cluster/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      var n = 0
      val ids = mutable.Map[Int, Int]()
      println(pathTpl.format(name, pathSep.keys.mkString(",")))
      spark.read
        .parquet(pathTpl.format(name, pathSep.keys.mkString(",")))
        .rdd
        .map(x => x.getInt(0))
        .distinct()
        .sortBy(x => x)
        .toLocalIterator
        .foreach {
          id =>
            n += 1
            ids.update(id, n)
        }
      dict.update(name, ids.toMap)
      println("dict", name, ids.size)
      trainLog :+= "%s=%d".format(name, ids.size)
    }
  }

  def initStringFeatureDictV6(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): Unit = {

    trainLog :+= "\n------dict size------"
    for (name <- dictStrNamesForV6) {
      val pathTpl = "hdfs://emr-cluster/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      var n = 0
      val ids = mutable.Map[String, Int]()
      println(pathTpl.format(name, pathSep.keys.mkString(",")))
      spark.read
        .parquet(pathTpl.format(name, pathSep.keys.mkString(",")))
        .rdd
        .map(x => x.getString(0))
        .distinct()
        .sortBy(x => x)
        .toLocalIterator
        .foreach {
          id =>
            n += 1
            ids.update(id, n)
        }
      dictStr.update(name, ids.toMap)
      println("dict-str", name, ids.size)
      trainLog :+= "%s=%d".format(name, ids.size)
    }
  }

  def initLongFeatureDictV6(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): Unit = {

    trainLog :+= "\n------dict size------"
    for (name <- dictLongNamesForV6) {
      val pathTpl = "hdfs://emr-cluster/user/cpc/lrmodel/feature_ids_v1/%s/{%s}"
      var n = 0
      val ids = mutable.Map[Long, Int]()
      println(pathTpl.format(name, pathSep.keys.mkString(",")))
      spark.read
        .parquet(pathTpl.format(name, pathSep.keys.mkString(",")))
        .rdd
        .map(x => x.getLong(0))
        .distinct()
        .sortBy(x => x)
        .toLocalIterator
        .foreach {
          id =>
            n += 1
            ids.update(id, n)
        }
      dictLong.update(name, ids.toMap)
      println("dict-long", name, ids.size)
      trainLog :+= "%s=%d".format(name, ids.size)
    }
  }
}
