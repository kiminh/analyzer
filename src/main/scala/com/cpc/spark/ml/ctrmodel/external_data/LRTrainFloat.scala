package com.cpc.spark.ml.ctrmodel.external_data

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.dnn.Utils.DateUtils
import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
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
object LRTrainFloat {

  private var trainLog = Seq[String]()
  private val model = new LRIRModel

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark: SparkSession = model
      .initSpark("[cpc-model] linear regression")

    val dictDays = args(0).toInt
    val appDays = args(1).toInt

    val date = args(2)
    val date3ago = DateUtils.getPrevDate(date,2)
    val hour = args(3)
    var parserArg = "ctrparser4"
    if (args.length >= 5){
      parserArg = args(4)
    }
    val tomorrow = DateUtils.getPrevDate(date, -1)

    // 按分区取数据
    val appPathSep = getPathSeq(date, hour, appDays)
    val dictPathSep = getPathSeq(date, hour, dictDays)

    println("appPathSep = " + appPathSep)
    println("dictPathSep = " + dictPathSep)

    initFeatureDict(spark, dictPathSep)
    initStrFeatureDict(spark, dictPathSep)

    val userAppIdx = getUidApp(spark, appPathSep).cache()

    // fym 190512: to replace getData().
    val trainSql =
      s"""
         |select * from dl_cpc.external_data_sample_test_qizhi where dt='${date}'
       """.stripMargin


    println("trainSql = " + trainSql)

    val trainRawDF = spark.sql(trainSql)

    val trainDF = getLeftJoinData(trainRawDF, userAppIdx).cache()


    // fym 190512: to replace getData().
    val testSql =
      s"""
         |select * from dl_cpc.external_data_sample_test_qizhi where dt='${tomorrow}'
       """.stripMargin


    println("testSql = " + testSql)

    val testRawDF = spark.sql(testSql)

    val testDF = getLeftJoinData(testRawDF, userAppIdx).cache()



    model.clearResult()

    var parser=""
    var parserName=""
    var parserDestFile=""
    if ("ctrparser8".equals(parserArg)){
      parser = "ctrparser8"
      parserName = "qtt-bs-ctrparser8-daily"
      parserDestFile = "qtt-bs-ctrparser8-daily.lrm"
    }else{
      parser = "ctrparser4"
      parserName = "qtt-bs-ctrparser4-daily"
      parserDestFile = "qtt-bs-ctrparser4-daily.lrm"
    }

    train(
      spark,
      parser,
      parserName,
      trainDF,
      testDF,
      parserDestFile,
      4e8
    )

    Utils
      .sendMail(
        trainLog.mkString("\n"),
        s"[cpc-bs-q] ${parserName} 训练复盘",
        Seq(
          "fanyiming@qutoutiao.net",
          "xiongyao@qutoutiao.net",
          "duanguangdong@qutoutiao.net",
          "xulu@qutoutiao.net",
          "wangyao@qutoutiao.net",
          "qizhi@qutoutiao.net",
          "huazhenhao@qutoutiao.net"
        )
      )

    userAppIdx.unpersist()
  }


  def getPathSeq(cur_date:String, cur_hour:String ,days: Int): mutable.Map[String, Seq[String]] = {
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.set(cur_date.substring(0, 4).toInt, cur_date.substring(5, 7).toInt - 1, cur_date.substring(8, 10).toInt, cur_hour.toInt, 0, 0)
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

  //用户安装列表特征合并到原有特征
  def getLeftJoinData(data: DataFrame, userAppIdx: DataFrame): DataFrame = {
    data.join(userAppIdx, Seq("uid"), "left_outer")
  }

  def train(spark: SparkSession, parser: String, name: String, trainDF: DataFrame, testDF: DataFrame, destfile: String, n: Double): Unit = {
    trainLog :+= "\n------train log--------"
    trainLog :+= "name = %s".format(name)
    trainLog :+= "parser = %s".format(parser)
    trainLog :+= "destfile = %s".format(destfile)

    val num = trainDF.count().toDouble
    println("train num", num)
    trainLog :+= "train total size %.0f".format(num)

    // 最多n条训练数据
    var trainRate = 0.9
    if (num * trainRate > n) {
      trainRate = n / num
    }

    // 最多1000w条测试数据
    var testRate = 0.09
    if (num * testRate > 1e7) {
      testRate = 1e7 / num
    }

    val tnum = trainDF.count().toDouble
    val pnum = trainDF.filter(_.getAs[Int]("label") > 0).count().toDouble
    val nnum = tnum - pnum

    //保证训练数据正负比例 1:9
    val rate = (pnum * 9 / nnum * 1000).toInt
    println("total positive negative", tnum, pnum, nnum, rate)
    trainLog :+= "train size total=%.0f positive=%.0f negative=%.0f scaleRate=%d/1000".format(tnum, pnum, nnum, rate)

    val sampleTrain = formatSample(spark, parser, trainDF.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate))
    val sampleTest = formatSample(spark, parser, testDF)

    println(sampleTrain.take(5).foreach(x => println(x.features)))
    model.run(sampleTrain, 200, 1e-8)
    model.test(sampleTest)

    model.printLrTestLog()
    trainLog :+= model.getLrTestLog()


    val testNum = sampleTest.count().toDouble * 0.9
    val minBinSize = 1000d
    var binNum = 1000d
    if (testNum < minBinSize * binNum) {
      binNum = testNum / minBinSize
    }

    model.runIr(binNum.toInt, 0.95)
    trainLog :+= model.binsLog.mkString("\n")

    var dictLength = mutable.Map[String, Int]()

    dictLength.update("bias",1)
    dictLength.update("hour",24)
    dictLength.update("sex",9)
    dictLength.update("age",100)
    dictLength.update("os",10)
    dictLength.update("isp",20)
    dictLength.update("net",10)
    dictLength.update("cityid",dict("cityid").size + 1)
    dictLength.update("mediaid",dict("mediaid").size + 1)
    dictLength.update("slotid",dict("slotid").size + 1)
    dictLength.update("phone_level",10)
    dictLength.update("pagenum",100)
    dictLength.update("bookid",100)
    dictLength.update("adclass",dict("adclass").size + 1)
    dictLength.update("adtype",16)
    dictLength.update("adslot_type",10)
    dictLength.update("planid",dict("planid").size + 1)
    dictLength.update("unitid",dict("unitid").size + 1)
    dictLength.update("ideaid",dict("ideaid").size + 1)
    dictLength.update("appIdx",1001)
    dictLength.update("phone_price",1)
    dictLength.update("userid",dict("userid").size + 1)
    dictLength.update("brand",dictStr("brand").size + 1)
    dictLength.update("channel",dictStr("channel").size + 1)
    dictLength.update("dtu_id",dictStr("dtu_id").size + 1)
    dictLength.update("media_type",5)
    dictLength.update("province",40)
    dictLength.update("city_level",10)
    dictLength.update("interaction",5)
    dictLength.update("is_new_ad",2)


    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    val lrfilepathBackup = "/home/cpc/anal/model/lrmodel-%s-%s.lrm".format(name, date)
    val lrFilePathToGo = "/home/cpc/anal/model/togo/%s.lrm".format(name)

//    // backup on hdfs.
//    model.saveHdfs("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata/%s".format(date))
//    model.saveIrHdfs("hdfs://emr-cluster/user/cpc/lrmodel/irmodeldata/%s".format(date))
//
//    // backup on local machine.
//    model.savePbPackNew(parser, lrfilepathBackup, dict.toMap, dictStr.toMap, dictLength.toMap)
//
//    // for go-live.
//    model.savePbPackNew(parser, lrFilePathToGo, dict.toMap, dictStr.toMap, dictLength.toMap)

    trainLog :+= "protobuf pack (lr-backup) : %s".format(lrfilepathBackup)
    trainLog :+= "protobuf pack (lr-to-go) : %s".format(lrFilePathToGo)

    /*trainLog :+= "\n-------update server data------"
    if (destfile.length > 0) {
      trainLog :+= MUtils.updateOnlineData(lrfilepath, destfile, ConfigFactory.load())
      MUtils.updateMlcppOnlineData(mlfilepath, "/home/work/mlcpp/data/" + destfile, ConfigFactory.load())
    }*/
  }

  def formatSample(spark: SparkSession, parser: String, ulog: DataFrame): RDD[LabeledPoint] = {
    val BcDict = spark.sparkContext.broadcast(dict)
    val BcDictStr = spark.sparkContext.broadcast(dictStr)

    ulog.rdd
      .mapPartitions {
        p =>
          dict = BcDict.value
          dictStr = BcDictStr.value

          p.map {
            u =>
              val vec = parser match {
                case "ctrparser4" =>
                  getCtrVectorParser4(u)
                case "ctrparser8" =>
                  getCtrVectorParser8(u)
              }
              LabeledPoint(u.getAs[Int]("label").toDouble, vec)
          }
      }
  }

  var dict = mutable.Map[String, Map[Int, Int]]()
  var dictStr = mutable.Map[String, Map[String, Int]]()

  val dictNames = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid",
    "userid"
  )


  val dictStrNames = Seq(
    "brand",
    "channel",
    "dtu_id"
  )

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

  def initStrFeatureDict(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): Unit = {

    trainLog :+= "\n------dict size------"
    for (name <- dictStrNames) {
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
    i += 16

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
    /*val appIdx = x.getAs[WrappedArray[Int]]("appIdx")
    if (appIdx != null) {
      val inxList = appIdx.map(p => (p + i, 1d))
      els = els ++ inxList
    }
    i += 1000 + 1*/

    println("Vectors size = " + i)

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }


  def getCtrVectorParser8(x: Row): Vector = {

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    //bias
    els = els :+ (0, 1d)
    i += 1

    //hour
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

    //cityid
    els = els :+ (dict("cityid").getOrElse(x.getAs[Int]("city"), 0) + i, 1d)
    i += dict("cityid").size + 1

    //mediaid
    els = els :+ (dict("mediaid").getOrElse(x.getAs[String]("media_appsid").toInt, 0) + i, 1d)
    i += dict("mediaid").size + 1

    //adslotid
    els = els :+ (dict("slotid").getOrElse(x.getAs[String]("adslotid").toInt, 0) + i, 1d)
    i += dict("slotid").size + 1

    //phone_level 0 to 4
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

    //adclass
    val adcls = dict("adclass").getOrElse(x.getAs[Int]("adclass"), 0)
    els = els :+ (adcls + i, 1d)
    i += dict("adclass").size + 1

    //adtype
    els = els :+ (x.getAs[Int]("adtype") + i, 1d)
    i += 16

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

    val floatColumns="MD002F,G20805_4_30,7005007_0_7,7008006_0_7,7009002_4_7,7009005_1_7,7019101_4_7,7002003_3_7,G20306_4_7,G21704_1_30,G40008_2_30,7015010_2_7,721,535,G20307_3_30,338,G20803_0_7,G20205_4_7,G21001_1_7,7010_003,7019121_1_7,F1043,699,G21401_2_7,G20717_3_7,103,MD04AK,CF000,G20711_1_7,262,G40002_1_30,7014015_4_7,G21702_0_30,7014003_0_7,7013_0_7,277,MD004F,G20116_2_30,MD003T,7015013_4_7,7019106_4_30,G20306_0_30,D024,G21005_3_30,G20701_0_30,7019124_1_30,7008004_3_7,424,G21702_4_7,G20716_3_30,G40004_4_7,451,796,G21001_4_7,7009002_3_7,D019,G20600_1_7,G21403_1_7,MD0476,MD042M,G30009_4_7,MD048K,7019119_2_7,G20402_4_7,G20305_4_30,G20305_1_7,G30010_1_7,MD0496,7015_006,7014008_4_7,7015_2_7,365,G20206_3_30,7003_002,7008008_2_7,7011004_3_7,7019_132,7019128_1_7,7019102_3_7,G60001_0_30,830,7019129_2_30,670,7015003_2_7,7014008_1_7,G20122_1_7,G21706_0_30,746,848,7019123_3_30,G21706_4_7,G21708_3_30,MD04EK,G60002_4_30,7018003_3_7,7002009_3_7,MD04B9,7019122_0_7,G20711_4_30,MD04BV,310,7001001_0_7,G20603_2_7,MD048Z,G20205_0_30,5730_1000,MD009T,96,G21604_2_7,7015016_1_7,771,G20129_4_30,G20105_0_7,7003003_1_7,7019129_4_30,850,7019125_0_30,G30006_0_7,G20702_0_30,MD03GU,642,G20126_1_30,7014015_1_7,7019126_2_7,G20705_2_30,MD04DY,7019120_3_7,G30011_3_7,210,G20112_2_30,7010002_4_7,7006002_1_7,7019132_1_30,MD04CD,7019127_3_7,7019140_1_7,MD049O,MD04E5,MD04ER,121,G20304_2_30,7018_4_7,G21601_1_30,7018003_4_7,G40007_0_30,7006_001,7012011_4_7,7015_3_7,7012008_2_7,MD003B,7002001_0_7,7019_139,G60001_2_30,G40012_3_30,7019119_4_7,7004002_3_7,7019134_4_7,753,G20105_2_7,G21409_1_30,G21004_3_30,358,662,823,516,G30002_1_7,7005005_1_7,G20116_0_7,7019103_1_7,G20119_2_7,G20104_3_30,G20114_4_30,237,7005002_3_7,MD0482,G20715_3_7,G21101_2_7,G20805_1_7,G20701_4_7,G40002_3_7,7019115_0_7,G21102_0_7,7012001_2_7,7015018_0_7,7011_4_7,7007_001,7008_2_7,7019_114,G20804_2_30,507,MD0048,7014010_0_7,G21705_3_30,MD047V,7001_3_7,7015013_3_7,488,7014002_2_7,G20201_0_7,G20702_4_7,G20718_2_30,384,571,303,7019126_4_7,7019107_0_30,7016_006,MD0034,7008_3_7,7015020_3_7,G20110_2_7,331,7002008_0_7,G40011_4_30,816,G20129_1_7,MD04AD,G20902_3_30,7001_2_7,G20710_1_30,7004007_0_7,G20712_3_30,MD049V,7002006_1_7,G21200_2_30,G20203_3_7,7009_003,26,G20714_0_30,G20708_4_7,7019133_0_30,375,561,7007005_0_7,614,G20718_0_7,G20404_1_30,G20400_0_7,G20600_4_7,G20402_0_30,G20703_1_30,MD044S,7019115_2_30,G21003_3_30,G30004_0_7,G20115_2_30,MD00ML,7016009_1_7,LO000,G20103_3_7,70,MD01BG,G21701_3_7,G20107_0_30,MD04BO,G40004_1_7,G20719_2_7,244,G21300_4_7,G21503_3_30,G21000_3_30,G20800_0_7,G21001_0_30,284,MD04BH,44,591,7013004_4_7,7019122_2_30,G21700_4_30,G20114_1_7,G20112_0_7,803,7016001_1_7,7019134_2_7,G20302_1_30,MD002M,7015020_4_7,G20705_0_7,634,G20301_4_7,G30003_4_30,MD03GN,607,G21703_1_30,G20706_2_7,MD04B2,778,33,7012_001,G21100_1_7,7019141_4_30,217,G40001_4_7,G50002_1_30,7011006_1_7,7010004_0_7,G21202_3_7,7011003_4_7,7019116_3_30,55,G21003_0_7,G21100_0_30,G20123_3_30,F1023,7019104_0_7,G21202_1_30,162,G20713_1_7,G20604_0_30,7010003_3_7,7007003_3_7,G21501_3_7,G21002_2_7,7019136_1_7,G20204_1_7,7003_009,7004001_1_7,G20120_1_30,G20108_0_30,7015_013,G20125_1_7,110,G20109_0_7,7019118_4_30,G40001_2_7,7014017_0_7,7019111_0_30,785,7015002_1_7,G20400_2_7,7007_4_7,7019113_1_30,221,F1030,837,G20700_0_30,MD003M,7019_121,7019111_0_7,G21003_2_7,7015009_1_7,MD0028,7019111_2_30,139,G20108_2_30,G21603_0_7,7019109_3_7,G20200_3_7,G20713_1_30,G21701_1_30,7002006_4_7,543,7005001_1_7,7013004_1_7,G20500_1_30,7019141_0_7,MD0165,7011007_4_7,MD003I,7014009_3_7,G20125_4_30,MD0044,189,7010003_2_7,G20206_2_7,7019114_0_30,G21602_4_7,7001_009,7012011_1_7,7005001_4_7,G40005_1_30,7019136_1_30,462,7019138_2_7,844,7007006_4_7,7015017_2_7,G20712_2_7,7019108_0_7,7019133_1_7,G30002_2_30,G20111_3_7,7009004_0_7,7008007_1_7,G21002_2_30,7012001_3_7,7019138_3_30,MD0169,37,7016008_4_7,599,F1002,7019108_2_7,MD049D,431,G20707_2_30,7012008_3_7,G30004_4_7,292,7012010_0_7,7003004_3_7,MD047A,7010006_4_7,7006003_2_7,G20125_0_30,G30001_1_30,7019111_4_30,G21505_1_7,G20604_1_7,7004001_4_7,7016005_4_7,349,60,7014_009,179,MD047H,7014016_3_7,G21700_1_7,7016006_2_7,G40007_1_7,G20405_3_30,7019106_1_30,7013003_0_7,7006006_4_7,7019130_2_7,554,7015005_4_7,G20708_0_30,G20205_1_7,F1009,G20604_4_7,MD046O,7019138_4_7,7001005_0_7,7010005_1_7,7019101_2_7,MD04DR,G21100_4_7,471,MD04ED,289,782,7016002_4_7,603,7011004_2_7,G21603_2_30,MD04BK,630,G20200_3_30,G40006_2_30,G21503_2_7,G21403_4_30,550,MD04AV,MD04CK,MD046H,MD04AY,7016002_2_7,678,627,739,497,MD049A,7019104_2_30,G20500_4_30,MD04CR,52,G30010_2_30,30,MD047O,MD04AO,415,G30008_2_30,229,7019118_2_30,G20204_0_30,7004006_2_7,7012004_4_7,G21100_4_30,7002_005,7015014_2_7,7013_1_7,7019106_3_7,G20710_4_30,G20101_2_7,G20800_2_7,G20900_4_30,G20108_0_7,7019108_2_30,618,7005006_2_7,539,G20714_4_7,7019130_3_30,621,792,G21506_3_7,789,CA000,7015006_3_7,G20109_2_30,MD04AR,7019141_4_7,345,172,7006004_0_7,296,479,7005010_4_7,G40008_1_7,146,7019_118,299,G20713_4_30,7011_004,G21603_2_7,MD00G7,595,F1016,MD000H,G20603_3_30,7003003_4_7,7015009_4_7,7019116_3_7,G30009_2_7,G20700_4_7,G20708_2_30,7001008_0_7,G20706_3_30,MD0041,G20202_2_7,7015021_2_7,G20125_4_7,MD049R,7014_016,MD003F,547,MD049K,G20111_1_30,G20400_3_30,G21707_2_30,7019113_3_7,G60002_1_30,G21501_3_30,MD049H,G20700_1_7,G20308_3_7,7016008_2_7,G21101_3_30,153,7019_125,G21702_2_30,557,7019118_0_30,7015002_4_7,MD044O,G20405_0_7,G20718_4_7,G21505_4_30,7003007_3_7,74,G21200_1_7,233,7001003_4_7,7012007_4_7,7005002_2_7,G21401_3_30,MD004B,7015015_0_7,75,7005006_3_7,7019102_4_30,G21704_4_30,7019121_0_30,7002004_0_7,MD03GR,G21707_2_7,G20717_1_30,7019130_4_7,G21005_2_7,G20718_0_30,214,G21505_1_30,7019102_3_30,455,G40008_4_7,G20305_4_7,420,665,G21703_3_7,7019125_1_7,MD049Z,MD04EO,G60002_3_7,7002_010,G30004_2_7,827,7012_2_7,7006001_0_7,G20708_0_7,7012003_0_7,7012005_2_7,7013_002,G20122_4_7,G21400_2_30,7016_1_7,G20126_4_30,G40002_4_30,646,MD002B,40,G30011_1_30,G21200_2_7,G20308_1_30,7013006_2_7,F1037,484,G20115_0_30,G20902_2_7,7019104_0_30,MD04BD,306,7011007_3_7,G21409_1_7,G20719_2_30,446,G20710_3_7,7019141_2_7,G20205_4_30,7019126_0_7,G21708_2_7,241,7008004_2_7,G20405_2_7,G40009_1_30,G20106_3_30,7013001_4_7,7014004_1_7,594,G21700_1_30,281,MD04D6,G40001_3_30,7006_005,G20901_2_30,7006006_1_7,MD015V,703,7011008_3_7,G20714_1_7,G20115_4_7,361,F1013,MD00MI,674,G21601_1_7,7007_004,175,7015009_3_7,7019124_3_7,MD04EG,22,G21707_1_7,G20101_3_30,MD04AG,819,G20805_0_30,MD03GQ,7003009_0_7,G21001_2_30,718,7004_007,G20900_0_30,G20602_1_7,G20101_1_30,7006002_4_7,7005003_0_7,7009002_1_7,F1012,MD0038,176,G40004_2_30,7011007_2_7,7016005_3_7,G20716_3_7,493,7013006_0_7,7016007_0_7,7019_103,MD04DC,396,7015017_4_7,724,G20800_3_30,475,G40007_4_30,MD047K,610,266,G40011_1_30,G20707_0_7,125,7005_008,7012005_3_7,G20303_4_30,G20702_2_30,MD044L,MD003Q,G20113_0_30,MD01BD,234,7019122_0_30,768,G20803_2_7,826,G21403_4_7,378,G21401_0_7,G20104_2_7,G30003_1_30,MD04BY,7019137_0_7,G20801_3_7,G40010_3_30,7006003_4_7,F1005,7013007_0_7,7015017_3_7,7018003_2_7,G21601_4_30,388,G20109_2_7,7011008_2_7,7006_1_7,G21000_3_7,G30002_0_30,7005009_0_7,MD0024,7015007_2_7,G30005_4_7,7002003_2_7,185,G20202_3_30,F0004,532,7008003_1_7,G20126_3_7,G20114_1_30,G20203_3_30,688,7019_136,G21707_4_7,476,710,7007003_4_7,G20501_3_7,G40005_3_7,165,198,G20112_2_7,MD048H,G30008_1_7,MD04EY,369,7015010_3_7,G20100_3_7,7018_2_7,7008007_4_7,7013001_1_7,7016_0_7,G21600_1_30,7012013_0_7,G21004_3_7,G20900_3_7,G21506_1_30,G20719_0_7,7014_005,7003001_0_7,G21706_1_7,G21602_0_30,7019109_3_30,G40004_4_30,654,114,224,7005009_2_7,7019123_2_7,G20705_2_7,G40009_2_7,617,MD0473,7008002_0_7,G20304_1_7,313,81,G20307_3_7,7003008_2_7,428,MD0031,G20704_4_30,MD002C,124,7019133_2_30,G20123_0_7,465,7019139_1_30,G20401_1_30,G20711_1_30,7001_002,405,7018_3_7,7013001_3_7,7019134_4_30,7015020_1_7,G20900_4_7,439,G21403_0_30,7019117_3_7,7007003_2_7,551,7002_001,7019119_3_30,7012008_4_7,7006005_0_7,7019128_1_30,G30009_3_30,G20605_4_30,MD04BL,855,G20113_4_7,G20108_1_7,G20300_2_30,7012012_2_7,MD047D,7009_1_7,MD0499,7001009_1_7,G21101_1_30,7005_2_7,G20107_4_7,7019137_2_30,MD048S,713,LG000,7016003_2_7,G20123_2_7,G21504_2_30,G40003_2_7,G21401_2_30,764,7013005_1_7,G21001_4_30,354,7001003_2_7,G40008_0_7,G20704_2_30,G20301_0_7,7008_004,7019116_2_7,274,248,G20802_3_30,G30007_3_7,G20100_4_30,MD048A,G21503_3_7,MD04DG,300,7014001_4_7,658,7014009_2_7,7015020_2_7,7015001_4_7,G20713_0_30,MD00G8,814,G20501_3_30,195,MD04EV,7008004_4_7,7012009_0_7,7008001_4_7,7016008_1_7,F1001,546,G20107_1_7,7014_001,MD04C2,G20707_4_7,7012004_1_7,845,G20123_2_30,7006006_3_7,G30011_4_30,6,65,7019108_3_30,MD04D2,7013005_4_7,G30006_0_30,409,G20300_2_7,7019135_3_7,7010005_4_7,7001_005,7006_0_7,7014017_2_7,7006002_3_7,7012012_3_7,G20124_3_7,7019108_4_7,G20602_4_30,7019121_1_30,G20709_3_7,G20502_2_7,525,761,7015008_0_7,G21300_0_30,7003_006,7013001_2_7,MD047G,7002002_4_7,G20715_1_30,728,7005_3_7,G40010_0_7,G20129_0_30,11,135,G21502_3_7,7019126_4_30,G20303_1_30,7007005_1_7,7008_008,MD048V,7015_021,G20103_4_30,MD048P,MD04A6,G20108_4_7,7019112_3_30,G20704_1_7,7019137_4_30,7019137_2_7,7016001_0_7,131,7019119_2_30,584,436,G20803_2_30,7001009_4_7,G20602_1_30,781,G20709_0_7,G21706_0_7,7006007_2_7,MD03GK,7007001_0_7,7015001_1_7,G30006_3_30,7019133_0_7,138,7016005_1_7,G21408_0_30,7016_002,7018001_0_7,G21504_1_7,7002005_4_7,7014001_3_7,MD004Q,7019115_3_30,7015011_0_7,48,G40006_0_7,G20801_1_7,7002002_1_7,G21705_3_7,MD0479,G21000_0_7,7019_110,7019105_4_7,G20600_0_30,7011007_1_7,14,G40006_2_7,G20121_2_30,78,7005010_3_7,251,7007002_1_7,7019140_2_30,7012_002,G21201_1_30,7010005_0_7,7001006_2_7,7014004_4_7,G20102_0_7,G20119_3_30,7008_001,7013002_0_7,G20108_4_30,MD046Z,7012_012,G20600_2_30,G20605_1_7,G20804_4_7,7011009_0_7,1,7009005_4_7,7019105_3_30,G20301_2_30,7001006_1_7,7019129_0_30,538,7014001_1_7,G20606_1_30,G20710_1_7,7014_008,G20901_0_7,88,G21102_0_30,206,LE000,MD048N,G20102_2_7,G21604_3_30,G21408_2_30,G21400_0_7,MD04ES,7012006_0_7,7018002_1_7,G20126_1_7,MD00SX,7001003_1_7,F0007,G20121_0_30,7014011_4_7,7004006_3_7,G20804_1_7,G20403_3_7,7019140_0_30,MD04C5,7013_005,7013_008,MD048D,7012_009,7007004_0_7,D008,G21102_2_30,G21707_0_7,MD015G,774,7011006_0_7,7008006_1_7,7009_002,7019140_0_7,G20601_1_30,G20605_4_7,G21706_2_30,G20301_0_30,85,G20121_0_7,258,7009_0_7,MD04A9,MD04BS,G20712_3_7,7015021_0_7,528,MD0492,G20702_0_7,G40012_0_7,G21400_0_30,731,MD04DJ,G60001_1_7,G21500_3_7,7012007_1_7,7019115_4_7,G20601_4_30,7016_007,7019126_2_30,F1031,7005010_1_7,G20120_0_30,G20405_2_30,G30007_4_30,MD04CZ,118,G20102_2_30,7001003_3_7,7007002_3_7,MD00ZA,7014011_1_7,7015_4_7,7019110_1_7,7019119_0_7,G20116_1_7,MD04D9,128,G21404_4_30,7016002_3_7,7012_3_7,MD0483,7015_014,7016004_0_7,7001006_4_7,G20128_3_30,G20306_2_30,G20712_0_7,G20801_4_30,G20116_4_7,7015001_0_7,G40011_0_30,7019129_0_7,7018_004,7007003_1_7,G20500_0_30,7019_140,7016_3_7,G30007_0_30,7011001_0_7,7019138_4_30,MD002W,G20103_1_7,822,7008_002,7019121_2_30,7011009_1_7,G20604_1_30,738,G21603_0_30,G21003_2_30,G20902_2_30,383,470,G21102_4_30,17,MD04B1,553,G21505_3_7,7011003_2_7,G20101_0_7,G20401_2_7,425,7016005_2_7,MD04CS,G20307_4_30,G20308_1_7,G20802_1_30,G20305_2_30,7019102_2_7,G21707_3_30,754,7014016_1_7,G21001_2_7,7015_007,G20402_2_7,G21708_0_7,G21201_4_30,F1008,MD04CC,7019134_0_30,G20202_0_30,7018_003,G20805_3_7,245,7014007_1_7,MD0033,276,7019107_1_30,G21005_0_7,201,G40009_0_7,G20301_1_30,7012006_1_7,7019128_3_7,G20400_3_7,795,7002005_1_7,G21005_2_30,661,7001_4_7,G20501_0_7,G20125_0_7,MD03GV,7009001_4_7,F1033,G21506_2_7,7005002_4_7,7019139_3_30,G20308_4_30,7003_1_7,7019141_3_30,7015018_2_7,MD04EL,G20128_2_7,G20901_4_7,7019127_4_7,7019114_2_30,7019113_1_7,544,7011010_0_7,707,G20801_3_30,MD04D1,7003_003,G21102_1_7,7014009_1_7,G20201_1_7,7010003_4_7,7012009_3_7,145,7019133_1_30,211,G30011_1_7,G20129_3_7,MD0027,MD04DZ,7019_131,669,MD043T,G20600_2_7,7012002_3_7,7006004_2_7,MD0477,433,G20603_3_7,7012011_3_7,7015021_4_7,G20109_3_30,G21502_1_7,F0008,7005007_2_7,7011008_0_7,MD049E,G21704_0_30,311,7019122_3_30,G20711_2_30,G21604_4_7,7019140_4_30,7005_0_7,MD046I,7012_011,G20706_3_7,7019118_4_7,G20105_1_7,MD003Y,178,7019_124,7001011_0_7,G20605_1_30,G20106_0_30,G21502_3_30,MD04A5,G20502_1_30,698,7002_006,7010_002,7005008_0_7,838,G50001_1_30,G40012_1_30,7008005_0_7,56,G40006_4_7,G20713_4_7,G20114_4_7,700,G20304_0_7,MD04C3,7003008_3_7,G21403_2_30,319,G21201_1_7,G30009_3_7,7002001_2_7,G20304_4_30,34,MD04D8,G40010_4_7,G21408_1_30,G21706_1_30,MD04EU,154,7019112_0_7,G20400_2_30,G21403_3_7,7009001_3_7,G40006_1_7,MD015J,G21501_2_7,59,575,MD04BW,7014004_0_7,7004002_1_7,G40003_1_30,763,MD0485,G20709_2_7,G20605_0_7,G20100_1_7,722,7008005_3_7,583,G21003_4_7,G20125_2_30,811,7005008_4_7,7003002_4_7,7003004_1_7,G20605_2_30,G20715_4_30,MD0045,G40004_2_7,G20704_1_30,MD002E,G30010_4_30,7014_017,MD04BA,170,770,7009004_1_7,G21708_2_30,G30008_4_7,MD04DX,7019136_3_7,G30004_2_30,G20305_3_7,129,G20109_4_7,MD04AA,705,7015008_4_7,G21505_2_30,G40001_3_7,F1015,MD048C,7001007_4_7,G20403_1_7,G20120_4_7,7019112_2_30,346,7019111_4_7,MD044P,G40005_3_30,G20901_3_30,7011005_3_7,G21504_2_7,G20121_3_30,G20711_0_7,G40011_1_7,7016003_0_7,51,7013004_2_7,831,G20124_4_7,G21102_2_7,247,269,624,G60001_1_30,F1038,MD00SW,G30011_2_7,MD0040,G20709_1_30,G30001_4_30,7015_005,G20202_3_7,7016009_3_7,7019129_1_7,7019106_3_30,7002008_2_7,7015005_3_7,G21002_0_30,G30003_1_7,MD047N,G20707_3_30,478,393,7003006_0_7,7013007_4_7,G50002_3_7,G20121_1_7,7019135_4_7,7009_007,G20116_2_7,G40009_3_7,F1006,G40007_2_30,7001011_3_7,G20707_0_30,7016_003,7005008_3_7,7004004_4_7,G21701_0_7,7012004_3_7,77,MD047U,7010_1_7,F1035,MD004P,317,487,G20306_2_7,7012007_2_7,G21002_3_30,G40008_3_30,736,G20120_3_7,7019107_2_30,MD044K,683,7019106_1_7,G40001_0_7,MD002Y,7006005_4_7,G21005_3_7,G21700_4_7,7004007_2_7,7019128_0_7,7003004_0_7,MD004I,802,608,7010_0_7,7019102_1_7,7019120_4_7,G21707_0_30,G21703_4_7,416,515,7009007_0_7,G30009_0_30,G20401_4_30,7001004_3_7,G20308_3_30,MD0025,G20109_1_7,851,824,427,G20600_4_30,MD047Y,491,7019134_3_30,327,G20604_2_30,7014008_2_7,G50001_2_7,G30001_4_7,7011_007,7005010_2_7,7016_008,G21102_3_30,MD04AU,341,364,7019107_2_7,MD047L,7002006_2_7,743,G20112_4_30,G21505_4_7,391,MD046N,7019128_2_30,G21201_2_7,MD04B8,MD04CN,7015019_3_7,G20300_3_30,G20122_4_30,615,7009007_3_7,240,7008_009,97,G50001_0_7,7003003_3_7,7007002_0_7,7012014_4_7,D011,G20604_0_7,G20125_1_30,7014_2_7,G21300_1_7,G21003_1_30,G20103_2_30,G20805_0_7,MD04EA,G30001_3_30,G20902_1_30,348,756,G21706_4_30,G20128_4_30,G20121_4_30,12,7019113_3_30,G40012_3_7,G20113_4_30,7015012_3_7,566,7004004_0_7,7012010_1_7,MD04BF,F0010,659,G40002_4_7,MD04E4,MD002P,7007005_4_7,339,G20128_1_30,7001005_1_7,188,7016004_1_7,7019107_0_7,7009003_4_7,G21700_0_7,7011010_2_7,7019112_1_30,G21101_0_7,7004001_3_7,G20713_3_7,7019135_0_7,7019122_1_7,F1022,G20302_3_30,MD0497,G20710_3_30,G30010_3_7,MD015L,F1019,G20112_4_7,401,G20305_0_7,G40012_2_7,7013003_1_7,7010004_3_7,7014016_0_7,MD047E,7015011_2_7,MD04AH,G21004_2_7,G60001_4_7,G30011_3_30,536,7015_012,466,7019139_0_30,MD04CH,G21101_0_30,7002009_4_7,7019127_2_7,7002_008,7003005_2_7,G20115_4_30,20,G20128_1_7,G40005_4_30,7019127_3_30,7008003_0_7,MD002I,7006006_2_7,G20804_4_30,MD04CU,G21300_1_30,7002003_4_7,7019135_2_7,F1026,G20206_1_30,MD002L,335,7019120_3_30,LM000,7015006_1_7,MD0490,7003009_2_7,MD00Z8,7019115_1_7,406,218,MD048E,7019101_0_30,27,7019121_0_7,7005001_0_7,G20403_2_7,G20401_1_7,7007004_3_7,7019107_4_7,136,7019140_1_30,7011004_4_7,7011001_3_7,G40003_2_30,7002007_1_7,MD04CA,560,G20715_3_30,G40012_2_30,7019134_3_7,729,MD00MK,7004_006,7004003_2_7,G20105_4_7,G20706_4_7,691,G21700_0_30,G20108_2_7,G21300_4_30,7001008_2_7,7002007_3_7,7002007_0_7,G20307_1_30,69,7005001_3_7,G20402_0_7,7015019_0_7,7008004_1_7,7018004_3_7,143,817,7015019_1_7,7019114_0_7,5,D023,G20112_1_7,G20304_3_7,7007_002,666,G20500_4_7,G20114_3_7,7012_004,G21700_2_30,7019_138,G20109_0_30,G20805_2_30,G20802_4_30,278,585,749,G20306_0_7,357,G20702_1_30,MD0035,MD04EN,7001011_1_7,G30009_0_7,G20115_1_7,G20601_1_7,G21403_1_30,7007_2_7,7011002_1_7,90,G21200_3_30,G21502_2_7,7015014_4_7,G50001_2_30,47,7015003_4_7,MD04CG,7019101_0_7,G21700_3_30,G20200_0_7,G20802_3_7,G21603_1_7,G20128_2_30,G20304_0_30,G20502_4_30,G20601_2_7,7005_007,7019136_0_7,G20901_4_30,783,G20604_3_7,CH000,7019_2_7,MD003P,MD0058,750,G20125_2_7,7013002_1_7,MD046U,7010006_3_7,7019106_4_7,7005008_1_7,MD048U,370,G20300_0_7,G20401_3_30,MD002K,G20202_0_7,MD047R,7002001_1_7,G20603_1_30,7001_011,7007004_4_7,MD04DD,334,7003006_4_7,7012014_2_7,G20602_3_7,G20308_0_30,7019135_2_30,7016004_3_7,186,7004_3_7,7019126_3_30,13,G20705_4_7,G40008_0_30,G30010_3_30,7011010_4_7,149,818,G20719_3_30,G20500_2_30,7019125_4_30,7019_111,G21506_4_30,MD049S,7012012_0_7,G20109_3_7,7018002_0_7,7019109_1_7,7018004_2_7,843,7006005_3_7,G20100_2_7,G21408_1_7,MD04BJ,7015012_1_7,G20711_4_7,G21604_2_30,567,7019134_2_30,G30002_1_30,F1020,408,MD047Z,MD04CO,39,7006003_3_7,G21002_0_7,116,7014012_4_7,G20802_1_7,447,7001006_0_7,G20602_0_30,7014_004,7004001_2_7,7014010_3_7,G21005_4_7,636,G20302_4_30,7019127_0_7,7015007_0_7,LJ000,7011_001,G20121_2_7,7010002_1_7,7015_020,G21505_3_30,156,297,G21200_3_7,7019103_2_30,742,G20112_0_30,259,7004001_0_7,G20502_2_30,MD003H,7009001_0_7,G30008_0_7,717,7013_4_7,7001004_1_7,G20128_0_7,7005002_0_7,G20716_2_7,342,7012006_2_7,G40008_2_7,G40012_4_30,7010001_1_7,G20709_1_7,G20805_1_30,637,7015018_3_7,MD04DL,G30003_2_7,G30003_3_30,MD04AP,G20711_0_30,486,G20707_1_7,G20709_3_30,7019117_4_30,G20305_0_30,G21000_2_30,604,150,G21102_4_7,712,7010005_3_7,7003007_4_7,7005007_3_7,G50002_1_7,G20103_4_7,7009004_2_7,7019115_2_7,G20120_4_30,MD004C,G20801_0_30,7015013_1_7,7014_010,7019103_4_30,76,G21101_4_7,MD048T,G60001_0_7,645,7013004_0_7,7019101_3_30,853,7009001_1_7,G21601_3_7,7012013_2_7,7019134_0_7,G40011_3_7,7019135_3_30,609,CB000,7009_006,7012006_4_7,G20604_4_30,7009003_2_7,MD003N,G50001_1_7,7004005_4_7,MD04BE,815,G20109_4_30,232,G20605_0_30,7001002_4_7,G20122_2_30,G21706_3_30,G20601_0_30,7008009_0_7,G20901_1_7,7019120_0_30,684,G21100_0_7,7011003_1_7,G20107_0_7,7019115_3_7,MD049Y,7010_006,7001002_2_7,MD04E9,G20129_4_7,MD00MJ,G20401_0_7,MD047S,MD04DK,142,7014_4_7,7001_004,G20304_1_30,7014_018,G20717_3_30,7002010_4_7,G40002_2_30,7019120_1_30,7019135_1_7,7002006_3_7,7019102_2_30,7019127_2_30,7008005_2_7,46,117,MD00G6,558,G20717_4_7,7015005_1_7,7014_002,G20202_2_30,MD0491,7012001_4_7,7011_3_7,7007_003,G21604_3_7,226,298,MD004H,775,7019140_4_7,7019114_4_7,G21708_3_7,G21603_3_30,196,7018003_1_7,7016003_3_7,G30011_0_7,7014002_1_7,MD049L,G20403_3_30,G21200_0_7,7010001_0_7,7004007_1_7,7018_0_7,MD049Q,MD003O,G20121_4_7,4,G20706_0_7,G20601_3_7,7002001_3_7,7011_008,7008005_4_7,G21201_4_7,G20200_2_30,7006_4_7,7019101_1_7,G20603_0_7,7004002_2_7,MD002J,7019_119,7003_3_7,D009,G20805_3_30,D006,7019136_2_30,G20709_4_30,MD04BR,7001007_2_7,G20306_1_7,537,109,7019_4_7,G40002_2_7,7019133_4_7,MD003A,498,G21504_4_7,7019106_0_30,400,7018004_4_7,122,G20603_0_30,7013_004,7019141_0_30,G20402_1_30,7003004_2_7,7015011_3_7,MD04DS,304,7002_007,G20715_0_30,G20603_2_30,MD04CV,G20306_3_30,7010_001,MD015K,G30006_2_30,7007004_2_7,G20710_0_7,7015013_2_7,7008004_0_7,7009003_1_7,89,7019_137,MD03GO,G20802_2_7,F0003,7012005_0_7,440,G21505_0_30,G21707_1_30,7019141_2_30,340,7019121_3_30,7019129_2_7,7019_104,G20717_4_30,G20101_3_7,G20206_2_30,7013002_3_7,21,7006_004,G20711_3_7,G21300_2_30,576,G21403_0_7,7016009_0_7,G20128_3_7,757,7014005_4_7,7019121_3_7,G20500_3_30,G20202_1_30,270,667,177,252,G20102_4_7,7019114_1_30,735,G20501_1_7,7002002_0_7,MD04BP,7019114_1_7,461,G20115_0_7,7015018_1_7,790,7010002_2_7,G21707_4_30,G20304_3_30,67,7002_1_7,G21201_3_30,MD04EZ,41,G20206_0_30,7006005_1_7,MD04C9,7019119_4_30,7006003_1_7,692,7005_001,G21200_0_30,G40008_1_30,F1027,G30004_3_30,G20402_1_7,G20125_3_7,MD048O,F1036,653,G20719_0_30,MD002Q,G20112_3_7,7004003_0_7,7005008_2_7,102,G20115_2_7,19,G30009_1_7,MD048J,7003001_4_7,MD009S,G20120_2_7,426,810,7019135_1_30,G20710_0_30,524,G20802_2_30,G20304_4_7,7012_005,7019101_3_7,598,G20902_0_30,G20500_3_7,706,G20200_1_30,G20803_4_30,7014016_2_7,G21708_4_7,MD04DF,G21505_0_7,G20306_3_7,G60002_4_7,G20403_1_30,631,7003009_1_7,G50001_3_30,7009006_3_7,318,7014010_2_7,7015014_0_7,552,7004_001,G20502_3_7,7016004_2_7,G50001_3_7,7019127_1_30,161,737,G20900_1_7,G21604_0_7,G20129_2_7,G21506_4_7,454,7008_1_7,7001010_3_7,7011002_0_7,7016004_4_7,7003005_0_7,G21201_3_7,291,265,G20403_4_30,G20302_4_7,G20713_2_30,G20801_1_30,G40005_2_30,G20400_4_7,202,7001010_1_7,7014015_3_7,7019108_3_7,G30009_1_30,685,G20401_3_7,7019112_4_30,G21600_0_7,676,283,G21502_1_30,MD0472,G20804_3_30,G21603_4_30,MD04EF,7015019_4_7,F1039,347,7014008_3_7,G20103_1_30,MD002X,7019_117,7019122_2_7,G20704_0_30,7009002_2_7,G21403_2_7,7019126_0_30,82,G40003_3_30,7011010_1_7,376,G21601_4_7,G30003_3_7,F1014,MD015D,7019109_2_7,G20502_3_30,7019133_4_30,223,638,7011006_3_7,G21700_3_7,362,7019127_1_7,G30007_4_7,392,G20305_1_30,MD047M,477,7019113_0_30,G20901_2_7,777,G21502_4_30,7014003_2_7,7012_0_7,G30003_0_30,7019113_2_30,509,MD04AC,7002007_2_7,G20203_1_30,7014011_0_7,7015_1_7,7011009_2_7,G20123_0_30,G21409_0_30,G20711_3_30,MD04AW,531,590,616,7016008_3_7,G21000_1_30,MD003U,7019112_4_7,G21101_3_7,MD002D,326,F1034,7018_001,7015006_2_7,G30002_2_7,G50002_2_7,7013005_2_7,G21300_2_7,797,7019107_1_7,MD004J,G20707_1_30,7005001_2_7,7005010_0_7,7009_3_7,G20112_3_30,G30008_0_30,G20802_0_7,MD0486,419,G20103_2_7,G20713_2_7,G20300_0_30,G20111_0_7,G20128_4_7,7019109_0_30,7019136_4_7,G40007_2_7,7005003_1_7,MD04ET,7014014_3_7,7004_002,7001010_0_7,7001005_2_7,7011005_4_7,7014011_2_7,7012010_2_7,G21701_2_30,7019141_1_30,137,G20712_1_7,7019_123,G20108_3_7,G20303_4_7,G20110_4_30,812,MD04A4,58,7019111_3_7,G21700_2_7,660,G20125_3_30,519,839,G40001_1_7,219,7019106_0_7,581,MD048Q,7010001_3_7,7019136_2_7,MD047F,G40009_1_7,7001003_0_7,7003009_3_7,7014_007,7013001_0_7,G30007_3_30,G50002_4_7,7006005_2_7,7003006_2_7,7015007_4_7,G20605_3_30,7019114_3_30,402,G21409_3_30,MD003Z,G40007_4_7,7012_003,G20400_1_7,7007001_3_7,G30004_4_30,MD04AB,G21100_3_30,G20709_4_7,320,G20107_3_7,7019138_1_7,G21503_1_7,MD046Q,G20801_0_7,G20604_3_30,7019104_3_7,7013003_2_7,G20114_2_7,328,MD044Q,MD002O,MD0026,G21504_3_7,MD049F,G21404_2_30,7008002_4_7,545,7019116_0_30,7011_006,G20123_1_7,G21502_2_30,G20103_0_7,7019110_2_7,7009006_0_7,G20400_4_30,G40006_3_7,7001005_3_7,7015019_2_7,MD0484,G21501_0_30,G21202_2_30,7015022_3_7,G21603_1_30,7002004_1_7,7019135_0_30,G20601_0_7,G20111_2_30,7015007_1_7,G20712_4_30,F0009,G20718_3_7,MD04C4,G21300_3_7,MD0498,7019115_1_30,652,7009_4_7,G30003_0_7,7004003_1_7,G20102_1_30,7019117_2_7,G20109_1_30,G20119_4_30,87,G20606_2_7,7019113_0_7,112,7019139_2_30,762,7015012_2_7,7004003_4_7,7007001_4_7,7006_2_7,G20128_0_30,G20301_3_30,7019133_3_30,7002_003,380,7015015_3_7,G30005_2_30,MD04B0,182,G20115_3_7,G40010_3_7,7019_141,G21601_2_7,356,G20203_2_30,7019120_2_30,MD04CT,246,F1007,780,G21101_1_7,MD046J,854,490,G21602_3_7,7012003_3_7,7019139_4_30,7013003_3_7,7016_4_7,7012013_4_7,7019108_1_30,632,441,7007005_2_7,G40006_1_30,G20900_3_30,94,MD03GL,G20104_1_7,G20702_3_7,G21409_2_7,7019103_2_7,239,7016003_1_7,G20902_1_7,G21404_0_30,434,G21000_4_7,821,7001_007,G21004_0_30,G20502_0_30,7004005_0_7,7010006_0_7,7019132_4_30,7012010_3_7,MD003D,MD04BM,481,7015020_0_7,144,7014004_2_7,G20200_0_30,7012003_2_7,G20112_1_30,7009_001,G21401_4_30,G20101_1_7,510,690,7007003_0_7,G20104_4_30,G20714_3_7,G21005_1_7,7019_116,7004007_3_7,MD04E0,7011001_1_7,7019134_1_30,G20802_4_7,G20802_0_30,G20300_1_30,G20602_0_7,460,G21703_0_7,708,7005003_4_7,7012013_1_7,G20403_4_7,7019140_3_30,MD04CB,755,7010001_2_7,G40009_0_30,7019113_4_30,G30008_1_30,MD04D0,7004_1_7,MD0036,MD04EM,G20405_4_30,7014017_1_7,7015_015,F1025,7019120_0_7,G20303_2_30,7002006_0_7,7014012_0_7,G20716_1_7,MD047X,G20404_2_7,119,7016_009,7019139_0_7,G20801_2_30,7019102_0_30,MD04E7,G20105_4_30,730,7002004_4_7,192,MD04DI,7011008_1_7,7015_008,G20123_4_30,160,G20703_0_7,7006_003,7008005_1_7,G40001_0_30,715,G20305_3_30,644,798,7019_109,G20716_4_7,453,MD04AI,MD009V,7009001_2_7,G21003_4_30,697,769,7019126_1_30,612,7019103_4_7,7015008_2_7,7015013_0_7,G21703_2_30,533,G20705_3_7,7005_010,D029,G30006_3_7,G20106_4_30,MD00Z9,205,G20305_2_7,MD0478,MD04DB,7010004_4_7,723,G20120_0_7,701,MD04BQ,212,394,7005009_1_7,7002010_2_7,7009004_4_7,G40003_4_30,589,719,G20805_2_7,640,7019128_2_7,G20106_1_30,G21005_4_30,7001001_4_7,846,7019124_2_30,G20126_0_7,G20122_2_7,MD04D7,G40009_2_30,MD04D4,773,7014010_1_7,G20203_0_7,G21703_2_7,G21000_1_7,7006004_1_7,7001008_4_7,MD04BT,805,G20100_2_30,G20116_3_7,130,312,G20706_1_7,7012005_1_7,7008003_2_7,G20401_4_7,G20113_3_30,7014006_1_7,363,G20307_1_7,7012_010,7015018_4_7,F1032,G20716_4_30,G60001_3_7,G20201_4_30,G20126_2_7,7019127_4_30,282,7018001_3_7,G20401_2_30,7012008_0_7,7019121_2_7,G21705_0_30,7014007_3_7,MD042K,7011007_0_7,MD048I,G40012_4_7,565,648,7019122_1_30,7018002_2_7,G20307_4_7,7019_112,MD0494,G20203_0_30,G21404_4_7,7005_006,7013_007,7019129_3_7,7002009_0_7,7018003_0_7,G20703_2_30,7014013_4_7,G20403_0_30,7013_2_7,G40003_4_7,7019103_3_30,G20601_3_30,F0002,D005,G20401_0_30,260,MD0003,G20106_1_7,7002005_2_7,7012001_0_7,MD049X,390,7013007_3_7,MD0471,G20121_3_7,G20122_3_30,7019120_4_30,G20206_4_7,7015011_4_7,7019104_4_30,G20901_1_30,7013006_1_7,7014013_1_7,371,397,7003007_0_7,7019117_0_7,G20602_3_30,7016007_3_7,7019132_0_7,G21408_0_7,7001001_2_7,7008009_4_7,7012004_0_7,G30008_3_7,7019105_1_7,200,7007004_1_7,7013_003,7005007_1_7,7019105_0_30,G20110_1_7,7005004_3_7,G20600_3_7,7008_003,G30001_0_30,16,G21502_0_7,7019129_1_30,G50001_0_30,7015016_0_7,G20124_2_7,G21000_0_30,7019117_4_7,G40003_0_30,G21604_1_7,7004_0_7,7015002_0_7,7019124_4_30,G21706_3_7,MD00SV,G21201_0_30,G20107_1_30,MD002V,7016_004,G21600_0_30,G30011_0_30,G20121_1_30,G20709_0_30,7015009_0_7,7006007_1_7,G20704_3_30,7016008_0_7,G21400_1_30,7019135_4_30,7018_002,7015_022,7006001_3_7,126,G20307_2_30,7016002_0_7,G20201_3_7,MD048B,G21201_2_30,133,G20713_0_7,7019124_0_7,7019_105,G20900_0_7,G20115_1_30,123,7001004_4_7,7001_013,MD00UO,716,7019112_1_7,MD004O,704,7019110_4_7,LQ000,G20124_0_7,G40012_0_30,7005_002,526,MD0474,G21201_0_7,G30005_0_30,7019119_1_30,G40007_3_30,7014_003,G21600_2_30,G20102_3_7,8,7009007_4_7,7015001_2_7,445,G21604_1_30,7019107_3_30,F0005,7008006_2_7,G21408_3_30,7013006_4_7,7014003_1_7,G30006_4_30,7012_007,G20113_2_7,G20502_4_7,G20403_2_30,G20206_4_30,268,7012014_3_7,MD015I,208,7011004_0_7,7019124_2_7,G20100_0_7,448,MD048X,456,368,7008006_4_7,7014006_4_7,7001001_1_7,G40011_0_7,7001_003,MD00HY,275,7014003_3_7,MD04BB,7019132_2_30,7012002_1_7,G21404_1_7,G20119_1_7,G20104_0_30,G21102_3_7,197,253,80,7002002_2_7,G20105_3_7,7001008_1_7,7018004_1_7,7019114_2_7,G60001_3_30,G40009_4_7,G21500_2_30,7019137_3_7,MD0032,7012_014,573,MD04BX,438,7013005_0_7,7019138_0_30,7014015_0_7,7007005_3_7,7007002_2_7,7019111_1_30,272,629,290,G20703_2_7,G20717_2_30,MD0163,MD0488,108,7019132_2_7,G20707_3_7,91,7019141_3_7,7014_011,G20103_0_30,MD04DE,G21701_4_7,MD047B,G20204_0_7,G21506_0_7,7011_009,711,7001_0_7,7010002_0_7,G20300_4_30,7007_1_7,7005_4_7,G20712_0_30,523,G20103_3_30,G20804_1_30,7012009_1_7,7004006_4_7,726,G20713_3_30,7008009_1_7,MD04AX,G30004_3_7,7014015_2_7,7003005_3_7,7011004_1_7,G20206_1_7,7008_006,7019108_1_7,G40011_2_7,G21500_4_7,G20715_0_7,7015010_4_7,G20108_3_30,808,G21202_0_30,7002008_1_7,7019106_2_7,MD002R,7015_019,G21003_3_7,G20107_4_30,7001001_3_7,7016003_4_7,G21002_1_7,LL000,7019113_4_7,7004002_0_7,7019137_1_30,7015021_1_7,7015006_0_7,776,G21401_1_30,G21504_1_30,7001009_2_7,7009005_3_7,G21101_2_30,G21601_2_30,578,83,MD0043,G20200_1_7,G20301_3_7,7001011_4_7,MD0480,791,597,G20202_1_7,463,G20403_0_7,G21503_4_7,7004_005,7019116_4_30,7010004_2_7,G21603_3_7,7006004_3_7,809,G20600_3_30,G20601_4_7,G21500_2_7,G20501_0_30,7014007_2_7,G20119_1_30,G20709_2_30,825,7011009_4_7,377,7019134_1_7,G21001_3_30,513,687,7008001_0_7,7002008_4_7,MD002Z,7009_005,7012011_0_7,G40008_3_7,G20114_0_7,7019118_3_30,G21000_4_30,53,343,G50002_0_7,G20605_2_7,G20400_1_30,141,7019109_2_30,499,G20706_4_30,G21707_3_7,32,360,835,G20102_4_30,G20303_1_7,7003003_0_7,38,7015_011,163,7010_2_7,577,7006_006,157,F1028,G20203_4_7,G20602_2_7,G20706_1_30,7015010_1_7,G20304_2_7,568,7002003_0_7,G30004_1_30,7019110_4_30,7019132_4_7,G40011_3_30,G50001_4_30,7014014_2_7,7012009_4_7,73,305,766,7019113_2_7,MD0039,G20605_3_7,271,MD044N,522,G20102_1_7,G40012_1_7,G20700_2_7,MD0487,MD049T,605,7019_126,MD046T,836,315,7019122_3_7,801,7003008_1_7,7009003_0_7,7010_3_7,7014017_3_7,G20718_1_30,7012007_3_7,MD003G,G20119_0_30,G21102_1_30,MD03GP,G20120_3_30,7003003_2_7,G21502_0_30,586,7001_010,G20201_1_30,7019102_0_7,G20715_2_30,G20300_1_7,G30003_4_7,G21300_3_30,MD04C8,G20901_3_7,399,G20111_4_7,G20710_2_7,MD04B7,G20712_4_7,7002008_3_7,G40003_1_7,444,7019133_3_7,G20804_2_7,MD049J,7019118_1_30,G30007_0_7,G30002_3_30,7015_018,7011_0_7,530,556,7001007_0_7,G21502_4_7,MD003V,758,7015005_2_7,7011008_4_7,7015015_4_7,G50002_2_30,7019127_0_30,7019108_4_30,G30011_2_30,7015005_0_7,G30005_1_7,MD004K,G30002_3_7,222,279,G21601_0_7,7016009_2_7,7001_012,MD048F,325,7015015_2_7,G20111_0_30,7019129_3_30,7014_1_7,G20704_2_7,G20702_3_30,7012001_1_7,204,7019107_3_7,541,G20706_2_30,7019116_1_7,G20123_1_30,134,G20129_3_30,677,G20110_1_30,MD004A,MD015E,7019126_1_7,7019112_0_30,7001009_3_7,336,G20604_2_7,G20501_4_7,G21705_4_7,7019121_4_7,264,7011005_2_7,7002_3_7,G21200_1_30,7005006_1_7,7003005_1_7,G20603_1_7,7019123_4_30,7014017_4_7,G20402_3_7,G40002_3_30,G20717_2_7,G20129_0_7,G21708_1_7,G21506_1_7,472,G20716_2_30,MD046F,G20205_3_30,G20705_4_30,7019141_1_7,7015014_1_7,7003008_4_7,G20719_1_30,F1018,7011002_3_7,G20603_4_7,45,G20101_2_30,7014014_1_7,7002005_3_7,151,G21600_1_7,MD04EE,G20705_1_30,G20308_0_7,748,G20717_0_7,7004003_3_7,G20716_0_30,G20803_1_30,7011003_0_7,G20708_3_7,7005005_0_7,MD04CI,7015004_3_7,MD002H,849,G20715_1_7,7012_006,G30004_1_7,7019101_1_30,MD049P,7019_133,7019_130,G20800_4_7,G60002_0_7,G21401_0_30,G20719_4_30,98,G20711_2_7,7019_0_7,7009007_1_7,101,G21409_0_7,7006003_0_7,209,7004004_2_7,7018002_3_7,7010001_4_7,G20100_4_7,G21403_3_30,7009002_0_7,7015007_3_7,MD04DW,115,G20104_1_30,7014005_0_7,G60002_0_30,411,G50002_3_30,559,620,744,7019128_4_7,G20500_1_7,G20701_3_30,LP000,MD04AQ,MD04CM,7015_001,7001_1_7,G20719_1_7,7019119_0_30,42,G20307_0_30,7002010_0_7,7003002_1_7,G20718_4_30,7005007_4_7,G20502_1_7,G21702_1_30,693,7018004_0_7,G20205_0_7,385,7003_004,G20705_1_7,G20308_2_7,G20129_2_30,167,231,28,7015011_1_7,7019_113,7015022_2_7,G20603_4_30,G21200_4_30,G20302_0_30,7019116_0_7,7015017_1_7,829,G21003_0_30,7019114_3_7,G20500_2_7,7012_4_7,G40010_4_30,G21501_4_30,7008009_3_7,7019123_1_7,MD004D,G30003_2_30,7011_002,7011_1_7,G21004_1_7,MD04CW,7012005_4_7,7019128_3_30,7005_009,G40005_0_30,333,751,G20307_0_7,G20714_2_7,734,MD047Q,G20800_4_30,MD04CF,G40011_2_30,7019106_2_30,682,7015004_4_7,7011002_4_7,G21706_2_7,G21506_2_30,7015012_4_7,7019105_4_30,F1021,G21004_4_7,G20707_4_30,7009005_2_7,LR000,G20202_4_30,105,G20302_2_30,G20114_2_30,MD04EB,7002002_3_7,216,7002_4_7,G21602_3_30,7005004_4_7,G20126_4_7,G21604_4_30,MD0495,MD04DM,457,261,G20200_4_7,7011002_2_7,D004,G20801_4_7,468,G30010_2_7,G20308_2_30,7014014_4_7,MD04EI,7019139_4_7,7006004_4_7,MD04E3,MD049C,G21503_4_30,741,G20115_3_30,G20710_4_7,7002_2_7,MD049M,MD04DT,G30010_0_7,F1042,G21708_4_30,7014011_3_7,95,7010003_1_7,549,7015022_4_7,635,804,G21501_4_7,7019109_0_7,G21701_1_7,F1011,G20306_1_30,227,MD00G5,G20120_2_30,G21002_4_30,G21004_4_30,25,G20718_3_30,7003005_4_7,7003002_3_7,504,7004006_1_7,7012002_2_7,G21708_1_30,148,7019125_4_7,MD046M,MD04B3,MD04CP,MD04E8,G21501_1_7,7015_004,7019_120,G21505_2_7,MD047J,7019130_1_7,G30011_4_7,G20200_4_30,842,G21704_3_30,MD04BI,G20716_0_7,7019136_3_30,613,61,G21002_1_30,CD000,MD047T,MD04AT,238,G21701_2_7,G20204_2_7,7012012_4_7,7019101_2_30,G21500_0_7,794,625,7008008_0_7,7014008_0_7,MD0046,832,G20101_0_30,7019116_2_30,G21001_1_30,50,G21202_0_7,G40008_4_30,7019120_1_7,35,7010_005,784,7015004_1_7,7008006_3_7,MD004G,517,7019125_3_30,MD04AJ,7012002_4_7,G30001_2_30,G20800_3_7,G20708_2_7,G20404_4_7,489,G21600_4_30,180,7012009_2_7,680,7006007_4_7,F0001,7013007_1_7,MD049N,7013004_3_7,G20703_3_30,MD04EC,G21503_0_7,302,7006_002,MD03GM,G20718_1_7,7016_001,G40004_3_7,7015010_0_7,7019132_3_30,7019130_0_7,7018002_4_7,G40006_0_30,7019108_0_30,G20106_4_7,MD003C,7012_1_7,7004002_4_7,7015014_3_7,7001007_1_7,7016002_1_7,7010_4_7,G21404_1_30,779,79,MD015A,G20501_4_30,G21504_0_7,745,7015003_0_7,7019121_4_30,G30002_0_7,G20123_3_7,633,7005004_1_7,G20107_2_7,G20700_1_30,600,7019118_2_7,G21504_4_30,G20104_0_7,120,G20302_2_7,43,7019_1_7,MD04DQ,G20204_1_30,G20716_1_30,480,MD00BL,7019105_2_30,MD04E6,G20717_1_7,G60002_2_30,7012012_1_7,G50002_0_30,7019123_1_30,G20203_4_30,MD04CL,7001_006,7008003_4_7,7011_005,7010006_1_7,G21300_0_7,MD049W,G21601_0_30,7009005_0_7,7019119_3_7,G21004_1_30,7019109_4_7,7014006_3_7,G30008_2_7,7006001_1_7,7007_3_7,7,MD046R,193,G20902_0_7,330,G40010_0_30,G20113_3_7,7019130_4_30,G20116_4_30,747,G21503_2_30,MD046P,7015002_2_7,G40007_1_30,7001_008,7008_4_7,G20705_3_30,847,572,G30005_0_7,G20404_0_30,G20110_2_30,MD004N,MD04CJ,G21409_4_7,G20124_0_30,7019116_1_30,7013_006,MD048Y,G20901_0_30,7012007_0_7,7011003_3_7,7014_0_7,852,G40010_1_7,G20606_3_7,7019111_2_7,G20111_3_30,412,442,D026,7004005_2_7,7019104_1_7,7002_004,7007006_2_7,7001006_3_7,G20202_4_7,7019125_1_30,7008003_3_7,G20803_1_7,321,G21404_3_30,152,G30002_4_30,7019137_1_7,7003009_4_7,7019138_2_30,520,MD04DA,G21603_4_7,813,7014012_2_7,G21500_1_7,G21202_4_30,G21100_2_30,G20704_0_7,7005002_1_7,7019110_0_30,7008009_2_7,7013006_3_7,7019139_1_7,7019124_0_30,MD0470,7010_004,7009006_1_7,7019137_4_7,7008007_0_7,7014_015,7001009_0_7,G20122_0_7,G21604_0_30,MD048R,7015008_1_7,G60002_3_30,MD003E,720,452,423,36,236,F1017,7015006_4_7,G20122_3_7,7019117_0_30,7016006_4_7,G20201_0_30,G20203_1_7,111,G20101_4_30,651,293,7019138_3_7,G20902_4_30,7019112_3_7,7019125_2_7,7019105_0_7,643,267,G30005_1_30,5729_1000,G21409_2_30,7015015_1_7,104,G20120_1_7,7012011_2_7,7008002_2_7,7019130_2_30,7003001_3_7,7019124_3_30,295,G21001_3_7,CG000,7003004_4_7,7016001_3_7,10,7010004_1_7,7003_001,806,G20804_0_7,7005_003,527,7019128_4_30,173,7016009_4_7,G20107_3_30,MD015H,7012003_4_7,G30007_2_30,MD04AS,G40002_0_30,254,CE000,MD0475,G20715_2_7,G50001_4_7,G20602_2_30,7005009_4_7,367,7014007_4_7,7014004_3_7,G20119_3_7,G21202_3_30,MD049G,G20710_2_30,228,G20110_0_7,MD003L,G21500_3_30,449,7019111_1_7,7001002_0_7,G40004_1_30,G30005_4_30,G20201_2_7,MD0047,G20700_0_7,G20102_3_30,G20402_3_30,MD015O,7008007_2_7,G20303_2_7,788,G21600_2_7,7012008_1_7,7016_2_7,496,MD04A3,G20126_0_30,7014002_0_7,580,G20606_4_7,G20204_4_7,G20100_3_30,7019_106,G21400_4_7,G21202_1_7,147,7005009_3_7,G20800_2_30,G20205_3_7,7012010_4_7,7005005_2_7,G21005_1_30,G20116_3_30,7019104_4_7,626,G20306_4_30,15,G40001_4_30,G20206_3_7,7005004_0_7,663,G60001_2_7,752,G20113_2_30,MD04A1,569,7019126_3_7,7002_002,7003_0_7,7015_009,G20800_0_30,MD04CE,G20405_0_30,MD04AN,G20719_3_7,MD0161,7019136_0_30,602,820,G20111_1_7,435,0,7001005_4_7,7019138_0_7,G20703_4_7,127,7014_013,7008001_3_7,G20805_4_7,G60001_4_30,MD046K,MD0481,7019116_4_7,7007_006,MD046G,G20119_4_7,309,7019104_3_30,671,430,7014013_0_7,7015016_3_7,G21702_2_7,7019118_1_7,372,G20105_0_30,7014_3_7,G21400_3_30,MD004L,7019132_1_7,786,7006002_2_7,G20902_3_7,7005_005,7015_016,G20204_3_7,G20703_0_30,7015004_2_7,7019105_1_30,G40004_0_7,7014009_0_7,MD00OB,113,G20606_2_30,G60002_1_7,529,332,7019123_4_7,G20706_0_30,256,G20714_1_30,G40009_4_30,MD003J,772,G20702_4_30,7001011_2_7,F1010,G20105_2_30,7006007_3_7,86,G30001_2_7,7011001_2_7,7001004_0_7,7016_005,84,G30005_3_30,MD04EJ,7004004_1_7,G20700_2_30,G21400_3_7,G40005_4_7,494,MD00SY,MD04EH,7019130_3_7,166,505,641,7014001_2_7,MD04DO,F1003,G20100_0_30,MD03GT,7019120_2_7,649,7005006_4_7,7004007_4_7,G21004_0_7,7003_4_7,5731_1000,7002003_1_7,673,MD04EQ,G20106_3_7,7019_122,MD04DV,G20405_1_30,7019140_3_7,MD04BU,G20601_2_30,G20703_4_30,106,7008008_1_7,93,7019115_0_30,G20201_4_7,G20712_2_30,7001004_2_7,611,7005003_2_7,421,MD015Z,799,G20200_2_7,G30010_1_30,588,G40005_2_7,G20701_4_30,7019110_3_7,MD002S,G21703_0_30,7002004_3_7,714,7008001_1_7,G20600_1_30,MD046E,G21409_3_7,G21500_0_30,159,7006006_0_7,MD003S,G21408_3_7,286,G21602_1_30,140,G20606_0_7,G20900_1_30,MD048L,G20104_2_30,G40005_0_7,G20600_0_7,G21705_2_30,G20126_3_30,G20110_4_7,G40003_3_7,7009_004,7001008_3_7,G20701_1_30,7004_003,7015012_0_7,MD04AL,G21000_2_7,7003_008,MD04DH,G21602_4_30,7007006_0_7,MD004E,592,G20606_0_30,7019_108,840,410,MD009U,G21601_3_30,MD04BG,7014005_2_7,MD01BF,G20804_3_7,324,7019117_3_30,MD003W,7003_005,G21602_1_7,7019132_0_30,G21705_0_7,7019110_3_30,G20803_0_30,G21100_1_30,G20801_2_7,7007_0_7,G20111_4_30,G20703_1_7,7012014_0_7,7002009_1_7,7019128_0_30,7019114_4_30,MD04C7,G21704_1_7,511,7015022_1_7,MD04AE,243,G20714_4_30,7009004_3_7,MD015F,100,7013003_4_7,482,G21704_4_7,7012004_2_7,7019103_3_7,G21504_3_30,7019_115,G20204_2_30,G20129_1_30,MD04EX,7019104_2_7,7019_135,G20800_1_7,G20203_2_7,7014009_4_7,7012014_1_7,MD049U,G21701_4_30,7016007_1_7,G40001_1_30,459,MD04BN,7014013_3_7,F1040,G21705_2_7,7014016_4_7,184,G20303_3_30,MD04E1,7019123_3_7,7007002_4_7,655,G30001_0_7,7019109_1_30,G21401_3_7,759,263,G20113_0_7,7009007_2_7,7019107_4_30,7014_006,MD04BZ,G21704_3_7,7014003_4_7,G40009_3_30,587,G20307_2_7,G20800_1_30,G20714_0_7,G20404_2_30,G20708_3_30,7014012_1_7,G20205_2_30,MD01BE,250,G20501_1_30,733,MD044M,MD04C0,548,7019_128,7012006_3_7,G20405_3_7,G21703_4_30,MD04CX,31,500,7003002_2_7,7001002_3_7,G20204_4_30,834,G30007_1_7,G20108_1_30,G20106_2_30,G30006_2_7,G21702_1_7,MD04DU,G20606_3_30,352,G21003_1_7,7019_127,G40001_2_30,164,7008_007,7016001_2_7,G20404_1_7,7006007_0_7,G20124_2_30,G21400_4_30,G20902_4_7,389,G21002_3_7,G20300_3_7,MD00ZB,694,F1004,G20111_2_7,G30006_4_7,7001010_4_7,7003007_1_7,7019117_2_30,7005005_4_7,MD0030,G40002_0_7,G21701_0_30,7012003_1_7,7016006_1_7,350,215,MD004R,7019118_0_7,767,7011001_4_7,MD04A8,G20206_0_7,MD046D,7019109_4_30,G20707_2_7,D012,MD04B5,7013002_4_7,7014006_0_7,619,G20704_3_7,7019103_1_30,MD04D5,G20717_0_30,280,MD047I,7019_102,7018001_1_7,7010006_2_7,MD003X,7019102_1_30,725,7008008_3_7,G21702_0_7,7019140_2_7,7015016_4_7,MD04B4,242,G21600_4_7,G30009_4_30,G21501_2_30,7015_002,G20712_1_30,387,G20302_0_7,7003002_0_7,7014006_2_7,7004005_3_7,F1029,G20301_4_30,G20703_3_7,7002004_2_7,G20402_4_30,D007,132,512,7016006_0_7,23,64,7003_2_7,G40005_1_7,7008_005,7003_007,7007_005,G20701_1_7,G30001_3_7,G20400_0_30,MD04A7,G21506_3_30,G40010_2_30,7004004_3_7,G20803_4_7,G20701_3_7,G20701_2_30,696,G20700_3_7,G30002_4_7,7002010_1_7,702,7002007_4_7,G20700_4_30,800,7002001_4_7,7015002_3_7,G20119_0_7,7004_2_7,G21100_2_7,G40004_3_30,G21503_0_30,7011005_1_7,G30010_0_30,7008002_1_7,353,G20105_1_30,107,G30001_1_7,7004_4_7,G30005_2_7,G20201_2_30,MD04C1,G40011_4_7,G20110_0_30,7019110_1_30,G40006_4_30,7010003_0_7,7014002_4_7,G20702_1_7,7015_010,7007006_3_7,MD04BC,G20124_3_30,7007001_2_7,G20104_4_7,G21004_2_30,G20502_0_7,695,29,7003006_3_7,7002009_2_7,54,437,G21101_4_30,G20126_2_30,7014014_0_7,7011_2_7,7015022_0_7,7007006_1_7,7019129_4_7,7015003_3_7,G20404_0_7,G20302_1_7,732,5732_1000,7005005_3_7,833,63,G21703_1_7,7019124_1_7,MD04CY,395,344,G20116_1_30,G20107_2_30,MD0493,MD0042,7015_003,7019111_3_30,249,7019125_3_7,24,G30008_4_30,MD03GS,7019137_3_30,7011_010,501,7014007_0_7,G20404_4_30,G40006_3_30,G20124_1_7,G20718_2_7,7008007_3_7,99,7019115_4_30,F0006,7019139_2_7,G21409_4_30,7004005_1_7,7019_101,760,7008_010,7016005_0_7,G21100_3_7,G20116_0_30,7011010_3_7,G20606_1_7,G20114_3_30,MD04C6,G20122_0_30,7014010_4_7,7015003_1_7,G20301_1_7,G21202_4_7,G21408_2_7,7019130_0_30,G21704_2_30,657,7019105_2_7,MD04E2,7019119_1_7,7019_134,7013_3_7,G21400_1_7,793,7019_129,7014012_3_7,G21702_4_30,F1041,MD04EW,740,G21501_0_7,7019124_4_7,7003001_2_7,7007001_1_7,MD0029,841,7001002_1_7,MD04CQ,7019125_2_30,7014_012,7011005_0_7,G20119_2_30,G40007_0_7,MD049B,207,257,7009006_4_7,308,7003006_1_7,7019110_0_7,G30010_4_7,MD04A0,G21404_3_7,7012_008,7009003_3_7,7008_0_7,7019101_4_30,G20714_2_30,765,G20303_0_7,MD046L,G20500_0_7,287,G30006_1_30,MD002T,G21708_0_30,MD048M,MD003R,7014001_0_7,7008008_4_7,G20501_2_7,G21401_4_7,7008002_3_7,G21506_0_30,521,7003008_0_7,D037,G20405_1_7,G21001_0_7,G20101_4_7,235,322,G20606_4_30,7016001_4_7,7005003_3_7,7015_0_7,G20704_4_7,7019136_4_30,199,7016006_3_7,G21501_1_30,288,G20303_0_30,G21702_3_7,G20300_4_7,7016007_2_7,G21408_4_30,7009_2_7,MD048W,158,7019112_2_7,G21701_3_30,7019130_1_30,555,650,458,G21704_2_7,7019139_3_7,MD04A2,G20405_4_7,7009006_2_7,G40002_1_7,G20204_3_30,7019123_2_30,7012_013,359,G40010_1_30,MD047P,72,G20404_3_30,G21600_3_30,7001007_3_7,MD002U,G20708_1_7,7003001_1_7,G20114_0_30,MD04DN,7001_001,379,7014_014,7019123_0_30,49,672,MD049I,G20104_3_7,G20124_4_30,MD044T,7014005_1_7,7012013_3_7,G21703_3_30,G20501_2_30,7019123_0_7,7006_007,G30007_2_7,G21602_2_30,7011_003,G20303_3_7,G21704_0_7,MD002A,7008001_2_7,MD046S,G21404_0_7,G20124_1_30,7018_1_7,7019110_2_30,7019125_0_7,G21705_1_7,G21202_2_7,443,MD04AF,MD04D3,G21500_1_30,593,828,G20708_4_30,7019105_3_7,G40007_3_7,7015017_0_7,G20308_4_7,220,7018001_4_7,G21401_1_7,7011006_4_7,7013_001,G21002_4_7,7019103_0_7,307,MD04DP,7015008_3_7,G21408_4_7,301,7011009_3_7,G20122_1_30,MD004M,7013002_2_7,G21500_4_30,7005004_2_7,G21400_2_7,MD0489,727,7005006_0_7,G20708_1_30,7006002_0_7,7006_3_7,MD002G,422,G30004_0_30,G21702_3_30,7001010_2_7,G21404_2_7,7004_004,MD04AZ,G20301_2_7,G20714_3_30,534,7019_107,G20113_1_7,7019133_2_7,G20804_0_30,7002_0_7,G20302_3_7,337,7013005_3_7,G21705_4_30,G21504_0_30,7015009_2_7,7019137_0_30,7019122_4_30,MD04AM,92,474,7013007_2_7,LN000,F1024,7011006_2_7,G21602_2_7,G20803_3_30,G30008_3_30,G60002_2_7,G20105_3_30,D039,G20102_0_30,373,MD04EP,G30007_1_30,7012002_0_7,464,495,G20900_2_30,MD0037,G20705_0_30,7010005_2_7,7014005_3_7,7018001_2_7,7006001_4_7,273,G20110_3_7,G21705_1_30,664,G40003_0_7,7019117_1_30,255,G20123_4_7,G20201_3_30,G21602_0_7,MD047C,174,7019132_3_7,7002010_3_7,7002005_0_7,G20702_2_7,562,7006001_2_7,7004006_0_7,G20106_2_7,MD048G,579,G21200_4_7,191,G20110_3_30,7019118_3_7,807,MD0167,429,G30009_2_30,G30006_1_7,7019104_1_30,7016007_4_7,542,606,7015_017,G21005_0_30,MD04B6,G20701_2_7,294,7005_004,G20404_3_7,G20602_4_7,7019138_1_30,MD003K,7019122_4_7,G20113_1_30,7015016_2_7,7015001_3_7,689,G20700_3_30,MD0049,G20205_1_30,MD002N,403,787,7019_3_7,G20900_2_7,G20402_2_30,MD047W,G21503_1_30,7019117_1_7,7019103_0_30,G40004_0_30,G20106_0_7,7015004_0_7,7002_009,7003007_2_7,G20715_4_7,7005_1_7,G20205_2_7,213,G30005_3_7,G21600_3_7,601,G20803_3_7,G50002_4_30,709,G20719_4_7,7019102_4_7,7015021_3_7,G20100_1_30,7014013_2_7,G40010_2_7,628,7010002_3_7,314,G20701_0_7".toLowerCase

    val floatColumnArray = floatColumns.split(",")

    println("floatColumnArray nums = " + floatColumnArray.size)

    for(col <- floatColumnArray){
      els = els :+ (i, x.getAs[Double](col))
      i += 1
    }

    println("Vectors size = " + i)

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  def getLimitedData(spark: SparkSession, limitedNum: Double, ulog: DataFrame): DataFrame = {
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
    for (t <- 0 to hours) {
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
}
