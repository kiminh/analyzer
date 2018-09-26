package com.cpc.spark.ml.dnn

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import mlmodel.mlmodel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Random
import com.cpc.spark.common.Murmur3Hash


object DNNSample {

  private var trainLog = Seq[String]()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val rnd = Seq("2Iul8CyRC31gUZOw5l0HCeay5AlTDj",
      "A+5dCdSXu5A18qG+fBmlXERin/mExnKBIc8z8iCQG8Y=",
      "QhiYaeEtqKLBKDSjjX0jflNEzi7Mc",
      "7tkS9tuY6D5xC5ORFrb3fTLEqea4RYoNwPpys6EzfvY=",
      "qO7UDw0Y9A+xSn1aiopNte9p5K8vWjGLvyb4OoW2p6k=",
      "xrYjb75xN+CQNjAZNaEoMTasM9o+aQmC2WXDrYJ/AjI=",
      "KOnEG28TunVoAq9Ms49xUSnPhqdn82cOurPzgI7fmTk=",
      "iKJ7abVqD2RumcdE0jXGNsUi0lHiXtNn4Kk8cz+Mk18=",
      "18b5FsGV5LsVkOPmKn7buvASGIKaZG7N9MirwSwxWqw=",
      "igvOdWF+cf47jXNoqJq7g1xO+VEkMLhxdyVHCC2ov2o=",
      "+uHW+UCdRxYipjGzXhyvmWJIWeasDJhQ2iJ4UHARfYQ=",
      "XYcIUrYw8YDpHVgKwe4Ma4eyyyElw/OGifDtmNy+ZVs=",
      "kEez3Ow3k62+Qeea8hCERDX2RNGYXJU35OoLwVHqrX0=",
      "IbWBG9yvQM9ro/S/PuZbnqglysDSy3TtzeqKPIOACco=",
      "F97rYPupYhh3H7BiyoO8NiLzyY6vjLy7LaMcbuEQ2QY=",
      "bLx2dGXP481I4H8nTZ43Xsu8QWc474L9/Ci+qiDNIUo=",
      "aEKQ3frWR5Q8pQ2vyP4GmvCVyaEOwxZ5F+31OBUsyX0=",
      "K9Sz5xpnZ9Yg4Z7Q7+YZRcbctVci",
      "isOTU6UDeyJk7vWlwg06OhhBY3oN3j7vJ/iGqE1j7vo=",
      "wfsnSseNdv59J4Mougdf7JkFgfMQJd3QOh+ZA5wqxao=",
      "diwul4OmHsK2XLeENu/nhXJG+cq75i+nYXFbQYsvLqY=",
      "pcLIparXy/OvorMjh8PpMqkkGxe9cuYzOxtkLFG0k2w=",
      "GWKSPEBGmgNkQIeKb+ASbVucI1RxHwXLYfkXYjJlRQM=",
      "V8O6vvxMut+JuaRibqTIRV58Ef7EMJRqph/o+Hf8Z8U=",
      "gOq9p3NqxZnTzNBm5o/DEc3ODtCm1u9rCt49FIizQ00=",
      "MI8h5zmBxnuE15miLcCqIbPa/Ubz7TLT5lW4glFjn+s=",
      "f7KQl81oQNfEt6WXE5YNj6tT0IDbNqQTnNjGw+7U/sI=",
      "GNbi7vC/L3gnnsJo/7U7BycxvsfEzYvgjZv14ktZSHU=",
      "z8sUfnqkPkKtqY9waaZBDKpf8XpbGlv9ToN/m/b3ulc=",
      "KMpbqCk8OTMxkDZtQ97IiVssjz0XQA1PwBeJ+dZs3uA=",
      "Vn/o1TR4wnrP331ebtvS5IrOs0ikEquRa3Ra2kvZZ+M=",
      "5ofLwqpCQU/0FhWo8UbeCqnFa2OnJjWgiCDN5XeDGDo=",
      "3Kb4pajHf/Co/8gmgGbyITtG86nLhn1w5swEFjL9o2U=",
      "xnW9bEPi58r5/7ph1o9priAbIM6c5m+13/99SvjG3m0=",
      "tAQBQEcGKh8G+nQfK9OjD6TQXZK2vwfHIAvRYQzTVFs=",
      "W+CrjyO1i2+wn+L9b2Id0wjUGBJG6iNsRluqm+e9GH8=",
      "eB/QPntj1BdZnNDWlBE8d0+bU+CbBvJZwBjE+e3yHtE=",
      "VU4xq67Cx4ZpziGS6l4lKHUPJ4z4ojpEowtkj0HX3uU=",
      "IyOfz7xM4KjYouMzP/Qwnf/EyY4YdAQ781foahTZzrc=",
      "crrwUUHnbt7j0ppDpZ2EMWTlQoz",
      "xx6XboYw1qZ+xeSsDv0oSlPdJFO/gZ5XWOI3P760Xx4=",
      "Uz9W4bwojMZv2D8L37sQyq4j8hWtwoRXwpuL2MUoc24=",
      "aNrrDXNWACF4ErhyxbqG4Aj/UBZel1P/krlKKArJlyk=",
      "4pjOEQL9rlnD/3Y1OQd2foweJokYDQar8OmovIx6D4s=")

    rnd.map(x => Murmur3Hash.stringHash64(x, 110)).foreach(println)

    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)

    //按分区取数据
    val ctrPathSep = getPathSeq(args(0).toInt)

    val userAppIdx = getUidApp(spark, ctrPathSep)

    val ulog = getData(spark,"ctrdata_v1",ctrPathSep)
      .filter {x =>
        val ideaid = x.getAs[Int]("ideaid")
        val slottype = x.getAs[Int]("adslot_type")
        val mediaid = x.getAs[String]("media_appsid").toInt
        ideaid > 0 && slottype == 1 && Seq(80000001, 80000002).contains(mediaid)
      }
      //.randomSplit(Array(0.1, 0.9), new Date().getTime)(0)
      .join(userAppIdx, Seq("uid"))
      .rdd
      .map{row =>
        val ret = getVectorParser(row)
        val raw = ret._1
        val apps = ret._2
        var label = Seq(0, 1)
        if (row.getAs[Int]("label") > 0) {
          label = Seq(1, 0)
        }

        var hashed = Seq[Long]()
        for (i <- raw.indices) {
          hashed = hashed :+ Murmur3Hash.stringHash64("%s:%d".format(fnames(i), raw(i)), 702)
        }

        val sparse = Sparse()
        sparse.idx0 :+= 0L

        var appHashed = Seq[Long]()
        for (i <- apps.indices) {
          appHashed = appHashed :+ Murmur3Hash.stringHash64("app:%s".format(apps(i)), 702)
        }

        sparse.idx1 :+= 0L
        sparse.idx2 ++= appHashed

        (label, hashed, sparse)
      }
      .zipWithUniqueId()
      .map(x => (x._2, x._1._1, x._1._2, x._1._3))
      .toDF("sample_idx", "label", "dense", "sparse")
      .repartition(1000)

    val clickiNum = ulog.filter{
      x =>
        val label = x.getAs[Seq[Int]]("label")
        label(0) == 1
    }.count()
    println(ulog.count(), clickiNum)

    val Array(train, test) = ulog.randomSplit(Array(0.97, 0.03))
    val resampled = train.filter{
      x =>
        val label = x.getAs[Seq[Int]]("label")
        label(0) == 1 || Random.nextInt(1000) < 100
    }

    resampled.coalesce(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnntrain-" + date)
    println("train size", resampled.count())

    test.coalesce(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnntest-" + date)
    test.take(10).foreach(println)
    println("test size", test.count())
  }

  def getPathSeq(days: Int): mutable.Map[String,Seq[String]] ={
    var date = ""
    var hour = ""
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -(days * 24 + 2))
    val pathSep = mutable.Map[String,Seq[String]]()

    for (n <- 1 to days * 24) {
      date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      hour = new SimpleDateFormat("HH").format(cal.getTime)
      pathSep.update(date,(pathSep.getOrElse(date,Seq[String]()) :+ hour))
      cal.add(Calendar.HOUR, 1)
    }

    pathSep
  }

  def getUidApp(spark: SparkSession, pathSep: mutable.Map[String,Seq[String]]): DataFrame ={
    val inpath = "/user/cpc/userInstalledApp/{%s}".format(pathSep.keys.mkString(","))
    println(inpath)

    import spark.implicits._
    spark.read.parquet(inpath).rdd
      .map(x => (x.getAs[String]("uid"),x.getAs[WrappedArray[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1,x._2.distinct))
      .toDF("uid","pkgs")
  }


  def getData(spark: SparkSession, dataVersion: String, pathSep: mutable.Map[String,Seq[String]]): DataFrame = {

    var path = Seq[String]()
    pathSep.map{
      x =>
        path = path :+ "/user/cpc/lrmodel/%s/%s/{%s}".format(dataVersion, x._1, x._2.mkString(","))
    }

    path.foreach{
      x =>
        println(x)
    }

    spark.read.parquet(path:_*)
  }

  val fnames = Seq(
    "hour", "sex", "age", "os", "net", "pl", "adtype", "city", "mediaid",
    "adslotid", "adclass", "planid", "unitid", "ideaid"
  )

  def getVectorParser(x: Row): (Seq[Int], Seq[String]) = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(x.getAs[Int]("timestamp") * 1000L)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var raw = Seq[Int]()

    raw = raw :+ hour

    val sex = x.getAs[Int]("sex")
    raw = raw :+ sex

    //age
    val age = x.getAs[Int]("age")
    raw = raw :+ age

    //os 96 - 97 (2)
    val os = x.getAs[Int]("os")
    raw = raw :+ os

    //net
    val net = x.getAs[Int]("network")
    raw = raw :+ net

    val pl = x.getAs[Int]("phone_level")
    raw = raw :+ pl

    val at = x.getAs[Int]("adtype")
    raw = raw :+ at

    val cityid = x.getAs[Int]("city")
    raw = raw :+ cityid

    val mediaid = x.getAs[String]("media_appsid").toInt
    raw = raw :+ mediaid

    val slotid = x.getAs[String]("adslotid").toInt
    raw = raw :+ slotid

    val ac = x.getAs[Int]("adclass")
    raw = raw :+ ac

    val planid = x.getAs[Int]("planid")
    raw = raw :+ planid

    val unitid = x.getAs[Int]("unitid")
    raw = raw :+ unitid

    val ideaid = x.getAs[Int]("ideaid")
    raw = raw :+ ideaid

    val apps = x.getAs[Seq[String]]("pkgs")

    if (apps.length > 0) {
      (raw, apps.slice(0, 500))
    } else {
      (raw, Seq(""))
    }
  }

  case class Sparse(
                   var idx0: Seq[Long] = Seq(),
                   var idx1: Seq[Long] = Seq(),
                   var idx2: Seq[Long] = Seq()
                   )
}




