package com.cpc.spark.small.tool

import java.text.SimpleDateFormat
import java.util.Calendar

import com.hankcs.hanlp.HanLP
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

object GetAddAge {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val isSave = args(1).toInt
    val day = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    val dayBefore = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val cal2 = Calendar.getInstance()
    cal2.add(Calendar.DATE, -(day + 10))
    val dayforetime = new SimpleDateFormat("yyyy-MM-dd").format(cal2.getTime)

    println("small tool GetAddAge run ... day %s-%s".format(dayforetime, dayBefore))

    val conf = ConfigFactory.load()

    val ctx = SparkSession
      .builder()
      .appName("small tool GetAddAge")
      .enableHiveSupport()
      .getOrCreate()

    val HASHSUM = 200000

    //    //-----刷数据
    //    for (i <- 1 to 15) {
    //      val day = i
    //      val cal = Calendar.getInstance()
    //      cal.add(Calendar.DATE, -day)
    //      val dayBefore = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //      println("small tool GetAddAge run ... day %s".format(dayBefore))

    if (isSave == 1) {
      val readdataDayBefore = ctx.sql(
        """
          |SELECT DISTINCT qkc.device,qc.title,sec.second_level
          |from rpt_qukan.qukan_log_cmd qkc
          |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
          |LEFT JOIN algo_qukan.algo_feature_content_seclvl sec ON qc.id = sec.content_id
          |WHERE qkc.cmd=301 AND qkc.thedate="%s" AND qkc.member_id IS NOT NULL
          |AND qkc.device IS NOT NULL
          |""".stripMargin.format(dayBefore)
      )
        .rdd
        .map {
          x =>
            (x.getString(0), x.getString(1), x.getString(2))
        }
        .groupBy(_._1)
        .map {
          x =>
            (x._1, x._2.map(_._2).toSeq, x._2.map(_._3).toSeq)
        }
        .filter(_._2.length >= 3)
        .map {
          x =>

            (x._1, (-1, x._2.mkString("@@@@"), x._3.mkString("@@@@")))
        }
        .repartition(50)
        .cache()

      readdataDayBefore
        .map {
          x =>
            val device = x._1
            val title = x._2._2
            val secondLevel = x._2._3
            "%s\t%s\t%s".format(device, title, secondLevel)
        }
        .saveAsTextFile("/user/cpc/wl/work/GetAddAge/readdata/%s".format(dayBefore))
      println("readdataDayBefore num: " + readdataDayBefore.count())
    }

    var fileNameArr = new Array[String](15)

    //生成数据时间范围数据
    for (i <- 1 to 15) {
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -i)
      val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      fileNameArr(i - 1) = day
    }

    println("fileNameArr", fileNameArr.mkString(","))

    val readdata = ctx
      .sparkContext
      .textFile("/user/cpc/wl/work/GetAddAge/readdata/{%s}".format(fileNameArr.mkString(",")))
      .map {
        x =>
          val arr = x.split("\t")
          var device = ""
          var title = ""
          var secondLevel = ""
          if (arr.length == 3) {
            device = arr(0)
            title = arr(1)
            secondLevel = arr(2)
          }
          (device, (-1, title, secondLevel))
      }
      .filter {
        x =>
          (x._1.length > 0 && x._2._2.length > 0)
      }
      .reduceByKey {
        (a, b) =>
          var title = a._2
          var secondLevel = a._3
          if (title.length > 0 && b._2.length > 0) {
            title = title + "@@@@" + b._2
            secondLevel = secondLevel + "@@@@" + b._3
          } else if (title.length == 0) {
            title = b._2
            secondLevel = b._3
          }
          (-1, title, secondLevel)
      }
      .repartition(50)
      .cache()

    println("readdata count", readdata.count())

    val unknownAge = ctx.sql(
      """
        |SELECT DISTINCT uid
        |FROM dl_cpc.cpc_union_log
        |WHERE `date`="%s" AND age=0 AND media_appsid in ("80000001","80000002")
      """.stripMargin.format(dayBefore))
      .rdd
      .map {
        x =>
          val device = x.get(0).toString
          (device, (1, "", ""))
      }
      .repartition(50)
      .cache()
    println("unknownAge count", unknownAge.count())

    val xdata = unknownAge
      .union(readdata)
      .reduceByKey {
        (x, y) =>
          var tag = x._1
          var titles = x._2
          var secondLevel = x._3
          if (tag == -1) {
            tag = y._1
          }

          if (titles.length() < 3) {
            titles = y._2
            secondLevel = y._3
          }

          (tag, titles, secondLevel)
      }
      .filter { x => (x._2._1 != -1 && x._2._2.split("@@@@").length >= 3) }
      .cache()

    val allData = xdata
      .map {
        x =>
          val deviceid = x._1
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

          (deviceid, Vectors.sparse(HASHSUM * 2, els))
      }
      .repartition(50)
      .cache()
    println("allData count", allData.count())

    val trainDataDocumentDF = ctx.createDataFrame(allData).toDF("deviceid", "features").repartition(50).cache()
    //val lrModel = LogisticRegressionModel.load("/user/cpc/wl/test/model/GetTrainUserAgeModel-lr-setRegParam057-setTol1E-18-setElasticNetParam00-100000-%d".format(1))
    val lrModel = LogisticRegressionModel.load("/user/cpc/wl/test/model/GetTrainUserAgeModel-lr-setRegParam042-setTol1E-18-setElasticNetParam00-100000-20180202-zfb-%d".format(5))

    val predictions = lrModel.transform(trainDataDocumentDF)
    val result = predictions.select("probability", "deviceid", "prediction")
      .rdd
      .map {
        x =>
          val arr = x.get(0).toString().replace("[", "").replace("]", "").split(",").map(_.toDouble)
          val threshold = arr.sortWith(_ > _).head
          val prediction = x.getDouble(2)
          val deviceid = x.get(1).toString()
          (deviceid, threshold, prediction)
      }
      .filter(_._2 >= 0.56)//0.49
      .repartition(50)
      .cache()
    println("result count", result.count())


    //    val agec0 = result.filter(_._3 == 0.0).count()
    //    val agec1 = result.filter(_._3 == 1.0).count()
    //    val agec2 = result.filter(_._3 == 2.0).count()
    //    println("agec0,agec1,agec2",agec0,agec1,agec2)
    //
    //    result.filter(_._3 == 0.0).saveAsTextFile("/user/cpc/wl/test/tmp/GetAddAge2-age0-1")
    //    result.filter(_._3 == 1.0).saveAsTextFile("/user/cpc/wl/test/tmp/GetAddAge2-age1-1")
    //    result.filter(_._3 == 2.0).saveAsTextFile("/user/cpc/wl/test/tmp/GetAddAge2-age2-1")
    // 三分类对应 adv 6分类
    //          1: 小于18 2:18-23
    //          3:24-30 4:31-40
    //          5:41-50 6: >50
    val sum = result
      .map {
        x =>
          val device = x._1
          val age = x._3.toInt
          val num = (new util.Random).nextInt(2)
          var randomAge = 0
          if (age == 0) {
            randomAge = num + 1
          } else if (age == 1) {
            randomAge = num + 3
          } else if (age == 2) {
            randomAge = num + 5
          }
          (device, randomAge)
      }
      .mapPartitions {
        p =>
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

          var n1 = 0
          var n2 = 0

          var age1 = 0
          var age2 = 0
          var age3 = 0
          var age4 = 0
          var age5 = 0
          var age6 = 0

          p.foreach {
            row =>
              val key = row._1 + "_UPDATA"
              val age = row._2

              if (age == 1) {
                age1 += 1
              } else if (age == 2) {
                age2 += 1
              } else if (age == 3) {
                age3 += 1
              } else if (age == 4) {
                age4 += 1
              } else if (age == 5) {
                age5 += 1
              } else if (age == 6) {
                age6 += 1
              }

              var user = UserProfile.newBuilder().setDevid(row._1)

              val buffer = redis.get[Array[Byte]](key).getOrElse(null)

              if (buffer != null) {
                n1 += 1
                user = UserProfile.parseFrom(buffer).toBuilder
              } else {
                n2 += 1
              }
              user.setAge(age)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
          }
          Seq((0, n1), (1, n2), (2, age1), (3, age2), (4, age3), (5, age4), (6, age5), (7, age6)).iterator
      }

    var n1 = 0
    var n2 = 0
    var age1 = 0
    var age2 = 0
    var age3 = 0
    var age4 = 0
    var age5 = 0
    var age6 = 0

    sum.reduceByKey((a, b) => a + b)
      .take(8)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 += x._2
          } else if (x._1 == 1) {
            n2 += x._2
          } else if (x._1 == 2) {
            age1 += x._2
          } else if (x._1 == 3) {
            age2 += x._2
          } else if (x._1 == 4) {
            age3 += x._2
          } else if (x._1 == 5) {
            age4 += x._2
          } else if (x._1 == 6) {
            age5 += x._2
          } else if (x._1 == 7) {
            age6 += x._2
          }
      }

    println("small tool GetAddAge n1: %d ,n2: %d,a1: %d,a2: %d,a3: %d,a4: %d,a5: %d,a6: %d".format(n1, n2
      , age1, age2, age3, age4, age5, age6))
    ctx.stop()

  }
}
