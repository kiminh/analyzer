package com.cpc.spark.ml.xgboost

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.util.Random
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Created by roydong on 19/03/2018.
  */
object SaveSampleSvm {

  var spark: SparkSession = null

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("xgboost get train sample")
      .enableHiveSupport()
      .getOrCreate()
    import sc.implicits._
    spark = sc

    var pathSep = Seq[String]()
    val cal = Calendar.getInstance()
    for (n <- 1 to args(1).toInt) {
      cal.add(Calendar.DATE, -1)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep = pathSep :+ date
    }

    val path = "/user/cpc/lrmodel/%s/{%s}/*".format(args(0), pathSep.mkString(","))
    println(path)
    var qtt = spark.read.parquet(path)

    if (args(2) == "qtt-list") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1).contains(x.getAs[Int]("adslot_type"))
      }
    } else if (args(2) == "qtt-content") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(2).contains(x.getAs[Int]("adslot_type"))
      }
    } else if (args(2) == "qtt-all") {
      qtt = qtt.filter{
        x =>
          Seq("80000001", "80000002").contains(x.getAs[String]("media_appsid")) &&
            Seq(1, 2).contains(x.getAs[Int]("adslot_type"))
      }
    }

    qtt = getLimitedData(2e8, qtt)
    qtt = intFreqTransform(qtt, "city", "citydx")
    qtt = stringFreqTransform(qtt, "adslotid", "adslotidx")
    qtt = intFreqTransform(qtt, "adclass", "adclassdx")
    qtt = intFreqTransform(qtt, "planid", "plandx")
    qtt = intFreqTransform(qtt, "unitid", "unitdx")
    qtt = intFreqTransform(qtt, "ideaid", "ideadx")

    qtt = qtt
      .map {
        r =>
          val vec = getVectorParser2(r)
          (r.getAs[Int]("label"), vec)
      }
      .toDF("label", "features")

    val Array(tmp1, tmp2) = qtt.randomSplit(Array(0.9, 0.1), new Date().getTime)
    val test = getLimitedData(1e7, tmp2)
    val totalNum = tmp1.count().toDouble
    val pnum = tmp1.filter(x => x.getAs[Int]("label") > 0).count().toDouble
    val rate = (pnum * 10 / (totalNum - pnum) * 1000).toInt // 1.24% * 10000 = 124
    println(pnum, totalNum, rate)
    val tmp = tmp1.filter(x => x.getAs[Int]("label") > 0 || Random.nextInt(1000) < rate) //之前正样本数可能占1/1000，可以变成占1/100
    val train = getLimitedData(4e7, tmp)

    test
      .map {
        x =>
          val label = x.getAs[Int]("label")
          val vec = x.getAs[Vector]("features")
          var svm = label.toString
          vec.foreachActive {
            (i, v) =>
              svm = svm + " %d:%f".format(i + 1, v)
          }
          svm
      }
      .write.mode(SaveMode.Overwrite).text("/user/cpc/xgboost_test_svm_v1")

    train
      .map {
        x =>
          val label = x.getAs[Int]("label")
          val vec = x.getAs[Vector]("features")
          var svm = label.toString
          vec.foreachActive {
            (i, v) =>
              svm = svm + " %d:%f".format(i + 1, v)
          }
          svm
      }
      .write.mode(SaveMode.Overwrite).text("/user/cpc/xgboost_train_svm_v1")
  }

  def getLimitedData(limitedNum: Double, ulog: DataFrame): DataFrame = {
    var rate = 1d
    val num = ulog.count().toDouble

    if (num > limitedNum) {
      rate = limitedNum / num
    }

    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
  }

  def intFreqTransform(src: DataFrame, in: String, out: String): DataFrame = {
    var n = 0
    var freq = Seq[(Int, Int)]()
    src.rdd.map(r => (r.getAs[Int](in), 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2)
      .toLocalIterator
      .foreach {
        x =>
          n = n + 1
          freq = freq :+ (x._1, n)
      }

    println(in, out, n)
    val dict = spark.createDataFrame(freq)
      .toDF(in, out)

    dict.write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
    src.join(dict, Seq(in), "left_outer")
  }

  def stringFreqTransform(src: DataFrame, in: String, out: String): DataFrame = {
    var n = 0
    var freq = Seq[(String, Int)]()
    src.rdd.map(r => (r.getAs[String](in), 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2)
      .toLocalIterator
      .foreach {
        x =>
          n = n + 1
          freq = freq :+ (x._1, n)
      }

    println(in, out, n)
    val dict = spark.createDataFrame(freq)
      .toDF(in, out)

    dict.write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
    src.join(dict, Seq(in), "left_outer")
  }

  def getVectorParser2(x: Row): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK) //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[Double]()

    els = els :+ week.toDouble
    els = els :+ hour.toDouble
    els = els :+ x.getAs[Int]("sex").toDouble
    els = els :+ x.getAs[Int]("age").toDouble
    els = els :+ x.getAs[Int]("os").toDouble
    els = els :+ x.getAs[Int]("isp").toDouble
    els = els :+ x.getAs[Int]("network").toDouble
    //    els = els :+ x.getAs[Int]("city")  //1111
    els = els :+ x.getAs[Int]("citydx").toDouble  //1111


    //    els = els :+ x.getAs[String]("media_appsid").toInt
    //    els = els :+ x.getAs[String]("adslotid").toInt  //1111
    els = els :+ x.getAs[Int]("adslotidx").toDouble   //1111
    els = els :+ x.getAs[Int]("phone_level").toDouble
    els = els :+ x.getAs[Int]("pagenum").toDouble

    try {
      els = els :+ x.getAs[String]("bookid").toDouble
    } catch {
      case e: Exception =>
        els = els :+ 0d
    }

    //    els = els :+ x.getAs[Int]("adclass")  //1111
    els = els :+ x.getAs[Int]("adclassdx").toDouble  //1111
    els = els :+ x.getAs[Int]("adtype").toDouble
    els = els :+ x.getAs[Int]("adslot_type").toDouble
    //    els = els :+ x.getAs[Int]("planid")  //1111
    els = els :+ x.getAs[Int]("plandx").toDouble  //1111
    //    els = els :+ x.getAs[Int]("unitid")  //1111
    els = els :+ x.getAs[Int]("unitdx").toDouble  //1111
    //    els = els :+ x.getAs[Int]("ideaid")  //1111
    els = els :+ x.getAs[Int]("ideadx").toDouble  //1111
    els = els :+ x.getAs[Int]("user_req_ad_num").toDouble
    els = els :+ x.getAs[Int]("user_req_num").toDouble

    Vectors.dense(els.toArray)
  }
}
