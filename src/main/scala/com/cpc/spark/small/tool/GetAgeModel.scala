package com.cpc.spark.small.tool

// $example on$
import com.hankcs.hanlp.HanLP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

/**
  * Created by wanli on 2017/10/11.
  */
object GetAgeModel {

  def checkValid(check: Int, age: Int, sex: Int): (Boolean) = {
    if (check == 1) {
      if (sex != 1 || (age != 1 && age != 2 && age != 3)) {
        false
      } else {
        true
      }
    }
    else if (check == 2) {
      if (sex != 2 || (age != 1 && age != 2 && age != 3)) {
        false
      } else {
        true
      }
    }
    else if (check == 3) {
      if (sex != 1 || (age != 4 && age != 5 && age != 6)) {
        false
      } else {
        true
      }
    }
    else if (check == 4) {
      if (sex != 2 || (age != 4 && age != 5 && age != 6)) {
        false
      } else {
        true
      }
    }
    else if (check == 5 && sex != 2) {
      false
    } else {
      true
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    val ctx = SparkSession
      .builder
      .appName("God bless")
      .enableHiveSupport()
      .getOrCreate()

    val HASHSUM = 200000

    //    var ageData = ctx.sql(
    //      """
    //        |SELECT DISTINCT info.device_code,zfb.info
    //        |from gobblin.qukan_member_zfb_log as zfb
    //        |INNER JOIN dl_cpc.qukan_p_member_info as info ON zfb.member_id = info.member_id
    //        |WHERE zfb.info like "%alipay_user_info_share_response%" AND zfb.info like "%person_birthday%"
    //        |AND info.day="2018-01-28" AND info.device_code is not null
    //      """.stripMargin)
    //      .rdd
    //      //.filter(x => x(1).toString().length > 0)
    //      .map {
    //      x =>
    //        var deviceid = x(0).toString()
    //        var birth = x.getString(1).split("person_birthday\":\"")(1).split("\"")(0)
    //        var age = -1
    //        if (birth >= "20100101") {
    //          age = -1
    //        } else if (birth >= "19900101") {
    //          age = 0
    //        } else if (birth >= "19750101") {
    //          age = 1
    //        } else {
    //          age = 2
    //        }
    //        (deviceid, (age, "", "", ""))
    //    }
    //      .filter(_._2._1 != -1)
    //      .repartition(50)
    //      .cache()
    //    println("ageData num is :" + ageData.count())
    //
    //    //
    //    //    var ageData = ctx.sql(
    //    //      """
    //    //        |select qpm.device_code,qpm.birth
    //    //        |from dl_cpc.qukan_p_member_info qpm
    //    //        |where qpm.day="2018-01-23" AND qpm.update_time>="2017-05-01" AND qpm.device_code is not null
    //    //        |AND birth is not null
    //    //      """.stripMargin)
    //    //      .rdd
    //    //      .filter(x => x(1).toString().length > 0)
    //    //      .map {
    //    //        x =>
    //    //          var deviceid = x(0).toString()
    //    //          var birth = x(1).toString()
    //    //          var age = -1
    //    //          if (birth >= "2010-01-01") {
    //    //            age = -1
    //    //          } else if (birth >= "1990-01-01") {
    //    //            age = 0
    //    //          } else if (birth >= "1975-01-01") {
    //    //            age = 1
    //    //          } else {
    //    //            age = 2
    //    //          }
    //    //          (deviceid, (age, "", "", ""))
    //    //      }
    //    //      .filter(_._2._1 != -1)
    //    //      .repartition(50)
    //    //      .cache()
    //    //    println("ageData num is :" + ageData.count())
    //    //
    //    //
    //    val readdata = ctx.sql(
    //      """
    //        |SELECT DISTINCT qkc.device,qc.title,sec.second_level
    //        |from rpt_qukan.qukan_log_cmd qkc
    //        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
    //        |LEFT JOIN algo_qukan.algo_feature_content_seclvl sec ON qc.id = sec.content_id
    //        |WHERE qkc.cmd=301 AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
    //        |AND qkc.device IS NOT NULL
    //        |""".stripMargin.format("2018-01-13", "2018-01-28"))
    //      .rdd
    //      .map {
    //        x =>
    //          (x.getString(0), x.getString(1), x.getString(2))
    //      }
    //      .groupBy(_._1)
    //      .map {
    //        x =>
    //          (x._1, (-1, x._2.map(_._2).toSeq, x._2.map(_._3).toSeq))
    //      }
    //      .filter(_._2._2.length >= 3)
    //      .map {
    //        x =>
    //          (x._1, (-1, x._2._2.mkString("@@@@"), x._2._3.mkString("@@@@"), ""))
    //      }
    //      .repartition(50)
    //      .cache()
    //    println("readdata num: " + readdata.count())
    //
    //    val ageReadData = ageData
    //      .union(readdata)
    //      //.union(extTitleData)
    //      .reduceByKey {
    //      (x, y) =>
    //        var age = x._1
    //        var titles = x._2
    //        var secondLevel = x._3
    //        //var extTitles = x._3
    //        if (age == -1) {
    //          age = y._1
    //        }
    //        if (titles.length == 0) {
    //          titles = y._2
    //        }
    //
    //        if (secondLevel.length == 0) {
    //          secondLevel = x._3
    //        }
    //        (age, titles, secondLevel, "")
    //    }
    //      .filter {
    //        x =>
    //          x._1.length > 3 && x._2._1 != -1 && x._2._2.length > 0 //&& x._2._3.length>0
    //      }
    //      .repartition(50)
    //      .cache()
    //    println("ageReadData count", ageReadData.count())
    //
    //    ageReadData
    //      .map {
    //        x =>
    //          val deviceid = x._1
    //          val age = x._2._1
    //          val titles = x._2._2
    //          val secondLevel = x._2._3
    //          "%s\t%d\t%s\t%s".format(deviceid, age, titles, secondLevel)
    //      }
    //      .saveAsTextFile("/user/cpc/wl/test/ageReadData-20180201-zfb-%d".format(1))

    val ageReadData = ctx
      .sparkContext
      //.textFile("/user/cpc/wl/test/ageReadData-20180201-zfb-%d".format(1))
      .textFile("/user/cpc/wl/test/ageReadData-20180128-zfb-2")
      .map {
        x =>
          val arr = x.split("\t")
          var deviceid = ""
          var age = -1
          var titles = ""
          var secondLevel = ""
          if (arr.length >= 3) {
            deviceid = arr(0)
            age = arr(1).toInt
            titles = arr(2)
            secondLevel = if (arr.length == 4) arr(3) else ""
          }
          (deviceid, (age, titles, secondLevel, ""))
      }
      .filter {
        x =>
          (x._1.length > 0) //&& (x._2._1 == 0 || (x._2._1 == 1 && (new util.Random).nextInt(2) == 1) || (x._2._1 == 2 && (new util.Random).nextInt(2) == 1))
      }
      .repartition(50)
      .cache()
    println("ageReadData count", ageReadData.count())

    val allData = ageReadData
      .filter {
        x =>
          (x._2._1 == 0 || (x._2._1 == 1 && (new util.Random).nextInt(2) == 0) || (x._2._1 == 2 && (new util.Random).nextInt(2) == 0))
      }
      .map {
        x =>
          val age = x._2._1
          val titles = x._2._2
          val secondLevel = x._2._3

          val terms = HanLP.segment(titles).filter { x => (x.length() > 1) && x.nature.toString().startsWith("n") }.map { x => x.word }.mkString(" ")
          val termarr = terms.split(" ")
          var done: Boolean = false
          var els = Seq[(Int, Double)]()

          for (i <- 0 to termarr.length - 1) {
            val tag = (MurmurHash3.stringHash(termarr(i)) % HASHSUM + HASHSUM) % HASHSUM
            if (els.exists(x => (x._1 == tag)) == false) {
              els = els :+ (tag, 1D)
            }
          }
          val extTermarr = secondLevel.split("@@@@")


          for (i <- 0 to extTermarr.length - 1) {
            val tag = (MurmurHash3.stringHash(extTermarr(i)) % HASHSUM + HASHSUM) % HASHSUM + 1 * HASHSUM
            if (els.exists(x => (x._1 == tag)) == false) {
              els = els :+ (tag, 1D)
            }
          }

          (age, Vectors.sparse(HASHSUM * 2, els))
      }
      .cache()
    //.randomSplit(Array(0.8, 0.2))
    //    //    //    xdata.map {
    //    //    //      x =>
    //    //    //        "%s\t%s".format(x._1, x._2.mkString(","))
    //    //    //    }.saveAsTextFile("/user/cpc/wl/test/userAgeTitleData-%d".format(1))
    //    //    //      val read = arr(2).split(",").map(_.toDouble)
    //    //    //      val pv = arr(3).split(",").map(_.toDouble)
    //    //    //      val plnet = arr(4).split(",").map(_.toDouble)
    //    //    //      val readchannl = arr(5).split(",").map(_.toDouble)
    //    //    //      (age, Vectors.dense(hour ++ read ++ pv ++ plnet))
    //    //    //  }
    //    //
    //    //val traindata = allData(0)
    //    //.cache()
    //.cache()
    //println("样本总数:" + testdata.count())
    //    println("测试总数:" + testdata.count())

    val tmpAllData = allData.randomSplit(Array(0.8, 0.2))
    val tmpAllData1 = tmpAllData(1).cache()
    val age0 = tmpAllData1.filter(_._1 == 0).count()
    val age1 = tmpAllData1.filter(_._1 == 1).count()
    val age2 = tmpAllData1.filter(_._1 == 2).count()
    println("age0, age1, age2", age0, age1, age2)

    val trainDataDocumentDF = ctx.createDataFrame(tmpAllData(0)).toDF("label", "features").repartition(50).cache()
    val testDataDocumentDF = ctx.createDataFrame(tmpAllData1).toDF("label", "features").repartition(50).cache()
    trainDataDocumentDF.show(20)

    //    for (i <- 1 to 20) {
    //      println("")
    //      println("")
    //      println("")
    //      println("-------------------------------------------------")
    //      println("for i is", i)
    val lr = new LogisticRegression()
      .setMaxIter(2000)
      .setTol(1E-18)
      .setRegParam(0.42) //0.57
      .setElasticNetParam(0.0)
      .setLabelCol("label")
      .setFeaturesCol("features")
    //.setFamily("multinomial")

    // Fit the model
    val lrModel = lr.fit(trainDataDocumentDF)
    //
    lrModel.save("/user/cpc/wl/test/model/GetTrainUserAgeModel-lr-setRegParam042-setTol1E-18-setElasticNetParam00-100000-20180202-zfb-%d".format(1))
    //
    val predictions = lrModel.transform(testDataDocumentDF)
    //
    val result = predictions.select("probability", "label", "prediction")
      .rdd
      .map {
        x =>
          val arr = x.get(0).toString().replace("[", "").replace("]", "").split(",").map(_.toDouble)
          val threshold = arr.sortWith(_ > _).head
          val prediction = x.getDouble(2)
          val label = x.getInt(1)
          (label, threshold, prediction)
      }
      .cache()

    for (a <- 1 to 100) {
      val tmp = result.filter(x => (x._2 * 100).toInt >= a).cache()
      val count = tmp.count()
      val ok = tmp.filter(x => (x._1.toDouble == x._3)).count()

      val age0 = tmp.filter(_._3.toInt == 0).count()
      val age1 = tmp.filter(_._3.toInt == 1).count()
      val age2 = tmp.filter(_._3.toInt == 2).count()

      val okage0 = tmp.filter(_._1 == 0).count()
      val okage1 = tmp.filter(_._1 == 1).count()
      val okage2 = tmp.filter(_._1 == 2).count()
      println(a, count, ok, age0, age1, age2, okage0, okage1, okage2)
    }

    val predictionAndLabels = predictions.select("prediction", "label").cache()

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    evaluator.setLabelCol("label")
    println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))
    evaluator.setMetricName("f1")
    println("Test set f1 = " + evaluator.evaluate(predictionAndLabels))
    evaluator.setMetricName("weightedPrecision")
    println("Test set weightedPrecision = " + evaluator.evaluate(predictionAndLabels))
    evaluator.setMetricName("weightedRecall")
    println("Test set weightedRecall = " + evaluator.evaluate(predictionAndLabels))
    println("")
    //}
    ctx.stop()
  }
}
