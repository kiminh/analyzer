package com.cpc.spark.ml.cvrmodel.daily

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.cvrmodel.daily.LRTest.model
import com.cpc.spark.ml.dnn.Utils.DateUtils
import org.apache.spark.sql.SaveMode

import scala.collection.immutable
import scala.sys.process._

//import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray

/**
  * Created by zhaolei on 22/12/2017.
  * new owner: fym (190520).
  */
object LRTrainEval {

  private var trainLog = Seq[String]()
  private val model = new LRIRModel

  private var dict: mutable.Map[String, Map[Int, Int]] = mutable.Map[String, Map[Int, Int]]()
  private var dictStr: mutable.Map[String, Map[String, Int]] = mutable.Map[String, Map[String, Int]]()
  private var dictLong: mutable.Map[String, Map[Long, Int]] = mutable.Map[String, Map[Long, Int]]()

  val dictNames: Seq[String] = Seq(
    "mediaid",
    "planid",
    "unitid",
    "ideaid",
    "slotid",
    "adclass",
    "cityid"
  )

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

    val days = args(0).toInt
    val date = args(1)
    val parser = args(2)

    var cvrPathSep = getPathSeq(date, days)
    println("cvrPathSep = " + cvrPathSep)

    val spark: SparkSession = model
      .initSpark("[cpc-model] qtt-bs-%s-daily".format(parser))

    val cachePrefix = "/tmp/cvr_cache/"

    if (parser == "cvrparser6" || parser == "cvrparser7") {
      initIntFeatureDictV6(spark, cvrPathSep)
      initLongFeatureDictV6(spark, cvrPathSep)
      initStringFeatureDictV6(spark, cvrPathSep)
    } else {
      initFeatureDict(spark, cvrPathSep)
    }

    val dates = nDayBefore(date, days)
    val dfPath = cachePrefix + date + "_" + parser + ".parquet"
    val idPath = cachePrefix + date + "_" + parser + "apIndex" + ".parquet"

    s"hdfs dfs -rm -r ${dfPath}" !

    s"hdfs dfs -rm -r ${idPath}" !

    val userAppIdx = getUidApp(spark, cvrPathSep)
    for (key <- dictStr.keys) {
      println(key)
    }

    model.clearResult()

    var name=""
    var destfile=""
    if ("ctrparser4".equals(parser)){
      name="qtt-bs-cvrparser4-daily"
      destfile="qtt-bs-cvrparser4-daily.lrm"
    }else if("cvrparser5".equals(parser)){
      name="qtt-bs-cvrparser5-daily"
      destfile="qtt-bs-cvrparser5-daily.lrm"
    }

    println("name = " + name + " , destfile = " + destfile)

    println("========= test ===========")
    //test
    val tomorrow=DateUtils.getPrevDate(date, -1)

    val queryRawDataFromUnionEvents =
      s"""select A.searchid
         |    , sex
         |    , age
         |    , os
         |    , network
         |    , isp
         |    , city
         |    , media_appsid
         |    , phone_level
         |    , `timestamp`
         |    , adtype
         |    , planid
         |    , unitid
         |    , ideaid
         |    , adclass
         |    , adslotid
         |    , adslot_type
         |    , brand
         |    , media_type
         |    , channel
         |    , sdk_type
         |    , dtu_id
         |    , interaction
         |    , pagenum
         |    , bookid
         |    , userid
         |    , siteid
         |    , province
         |    , city_level
         |    , doc_id
         |    , doc_cat
         |    , is_new_ad
         |    , uid,case when cv_types = null then 0
         |           when conversion_goal = 1 and B.cv_types like '%cvr1%' then 1
         |           when conversion_goal = 2 and B.cv_types like '%cvr2%' then 1
         |           when conversion_goal = 3 and B.cv_types like '%cvr3%' then 1
         |           when conversion_goal = 4 and B.cv_types like '%cvr4%' then 1
         |           when conversion_goal = 0 and is_api_callback = 1 and B.cv_types like '%cvr2%' then 1
         |           when conversion_goal = 0 and is_api_callback = 0 and (adclass like '11011%' or adclass like '125%') and B.cv_types like '%cvr4%' then 1
         |           when conversion_goal = 0 and is_api_callback = 0 and adclass not like '11011%' and adclass not like '125%' and B.cv_types like '%cvr%' then 1
         |      else 0 end as label from
         |(select
         |    searchid
         |    , sex
         |    , age
         |    , os
         |    , network
         |    , isp
         |    , city
         |    , media_appsid
         |    , phone_level
         |    , `timestamp`
         |    , adtype
         |    , planid
         |    , unitid
         |    , ideaid
         |    , adclass
         |    , adslot_id as adslotid
         |    , adslot_type
         |    , brand_title as brand
         |    , media_type
         |    , channel
         |    , client_type as sdk_type
         |    , dtu_id
         |    , interaction
         |    , interact_pagenum as pagenum
         |    , interact_bookid as bookid
         |    , userid
         |    , siteid
         |    , province
         |    , city_level
         |    , content_id as doc_id
         |    , category as doc_cat
         |    , is_new_ad
         |    , uid
         |    , conversion_goal
         |    , is_api_callback
         |  from
         |    dl_cpc.cpc_basedata_union_events
         |    where
         |    ((day = "$date" and hour >= "20") or (day = "$tomorrow" and hour <= "13"))
         |    and array_contains(exptags, 'bslrcvr=bs-v4-cvr')
         |    and media_appsid in ('80000001','80000002')
         |    and isshow = 1
         |    and isclick = 1
         |    and adsrc=1
         |    and charge_type = 1) A
         |  left outer join
         |   (
         |      select
         |      searchid, concat_ws(',', collect_set(cvr_goal)) as cv_types
         |      from
         |         dl_cpc.ocpc_label_cvr_hourly
         |      where
         |         `date`>="$date" and `date`<="$tomorrow"
         |      and label=1
         |      group by searchid
         |   ) B
         |   on A.searchid=B.searchid
         |
         """.stripMargin

    println("queryRawDataFromUnionEvents = " + queryRawDataFromUnionEvents)

    val df = spark
      .sql(queryRawDataFromUnionEvents)

    /*val ideaids = df
      .select("ideaid")
      .groupBy("ideaid")
      .count()
      .where("count > %d".format(minIdeaNum))

    val sample = df.join(ideaids, Seq("ideaid")).cache()*/

    val testDF = getLeftJoinData(df, userAppIdx)
    df.unpersist()

    train(
      spark,
      parser,
      name,
      testDF,
      destfile,
      1e8
    )

    Utils
      .sendMail(
        trainLog.mkString("\n"),
        s"[cpc-bs-q] ${name} 训练复盘",
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

    println(trainLog.mkString("\n"))

    s"hdfs dfs -rm -r $idPath" !

    s"hdfs dfs -rm -r $dfPath" !

  }

  /**
    * 取前n天的日期字符串
    * 自前一天起共n天
    * @param dateStart 开始日期
    * @param n 多少天
    * @return 日期字符串
    */
  def nDayBefore(dateStart: String, n: Int): immutable.IndexedSeq[String] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    1.to(n).map(c => {
      val cal = Calendar.getInstance()
      cal.set(dateStart.substring(0, 4).toInt, dateStart.substring(5, 7).toInt - 1, dateStart.substring(8, 10).toInt, 1, 0, 0)
      cal.add(Calendar.DAY_OF_MONTH, -c)
      dateFormat.format(cal.getTime)
    })

  }

  def getPathSeq(dateStart: String, days: Int): mutable.Map[String, Seq[String]] = {
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.set(dateStart.substring(0, 4).toInt, dateStart.substring(5, 7).toInt - 1, dateStart.substring(8, 10).toInt, 17, 0, 0)
    cal.add(Calendar.HOUR, -((days + 1) * 24 + 2))
    val pathSep = mutable.Map[String, Seq[String]]()

    for (n <- 1 to days * 24) {
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep.update(date, pathSep.getOrElse(date, Seq[String]()) :+ hour)
      cal.add(Calendar.HOUR, 1)
    }

    pathSep
  }

  def getPathSeq(days: Int): mutable.Map[String, Seq[String]] = {
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, 17, 0, 0)
    cal.add(Calendar.HOUR, -((days + 1) * 24 + 2))
    val pathSep = mutable.Map[String, Seq[String]]()

    for (n <- 1 to days * 24) {
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep.update(date, pathSep.getOrElse(date, Seq[String]()) :+ hour)
      cal.add(Calendar.HOUR, 1)
    }

    pathSep
  }


  def getUidApp(spark: SparkSession, pathSep: mutable.Map[String, Seq[String]]): DataFrame = {
    val inpath = "hdfs://emr-cluster/user/cpc/userInstalledApp/{%s}".format(pathSep.keys.mkString(","))
    println(inpath)

    import spark.implicits._
    val uidApp = spark.read.parquet(inpath).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs").rdd.cache()

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
      .flatMap(x => x.getAs[WrappedArray[String]]("pkgs").map((_, 1)))
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

  def isMorning(): Boolean = {
    new SimpleDateFormat("HH").format(new Date().getTime) <= "17"
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
    data.join(userAppIdx, Seq("uid"), "leftouter")
  }

  def train(
             spark: SparkSession,
             parser: String,
             name: String,
             testDF: DataFrame,
             destfile: String,
             n: Double
           ): Unit = {

    println("-------train log--------")
    trainLog :+= "name = %s".format(name)
    trainLog :+= "parser = %s".format(parser)
    trainLog :+= "destfile = %s".format(destfile)


    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser5-daily_2019-09-15-15-22")

    println("=========== tomorrow test ===========")

    val tomorrowTest = formatSample(spark, parser, testDF)
    model.test(tomorrowTest)

    model.printLrTestLog()
    trainLog :+= model.getLrTestLog()
    var testNum = tomorrowTest.count().toDouble * 0.9
    val minBinSize = 1000d
    var binNum = 100d
    if (testNum < minBinSize * binNum) {
      binNum = testNum / minBinSize
    }

    model.runIr(binNum.toInt, 0.95)
    trainLog :+= model.binsLog.mkString("\n")

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
                case "cvrparser5" =>
                  getCvrVectorParser5(u)
                case "ctrparser4" =>
                  getCvrVectorParser4(u)
                case "cvrparser6" =>
                  getCvrVectorParser6(u)
                case "cvrparser7" =>
                  getCvrVectorParser7(u)
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

  // 190511: baseline features.
  // bs-q项目最初的特征集, 从前同事的项目继承而来.
  def getCvrVectorParser4(x: Row): Vector = {

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

    println("Vectors size = " + i)

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  // 190523: baseline features + appidx + dnn features.
  // 增加了app特征 和 cqq带来的dnn特征.
  def getCvrVectorParser6(x: Row): Vector = {

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
    i += 11

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

    // 190523: dnn features

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

    println("Vectors size = " + i)

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  // 190520: baseline features w/appidx.
  // baseline特征+app特征.
  def getCvrVectorParser5(x: Row): Vector = {

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
    val appIdx = x.getAs[WrappedArray[Int]]("appIdx")
    if (appIdx != null) {
      val inxList = appIdx.map(p => (p + i, 1d))
      els = els ++ inxList
    }
    i += 1000 + 1

    println("Vectors size = " + i)

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

  // 190525: baseline features  + dnn features.
  // 增加cqq带来的dnn特征.
  def getCvrVectorParser7(x: Row): Vector = {

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
    i += 11

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

    println("Vectors size = " + i)

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }

}
