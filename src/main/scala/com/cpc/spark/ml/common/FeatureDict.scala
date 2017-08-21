package com.cpc.spark.ml.common

import java.io.PrintWriter

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source
import sys.process._

/**
  * Created by roydong on 22/06/2017.
  */
object FeatureDict {

  val adslot = mutable.Map[Int, Int]()

  val adclass = mutable.Map[Int, Int]()

  val city = mutable.Map[Int, Int]()

  val channel = mutable.Map[Int, Int]()

  val interest = mutable.Map[Int, Int]()

  val dict = Dict()

  def updateDict(ulog: RDD[UnionLog]): Unit = {
    /*
    adslot adclass city channel interest
     */
    val lastAdslot = ulog.map(x => (x.adslotid, 1))
      .reduceByKey((x, y) => x)
      .map(_._1.toString)
      .filter(_.length > 0)
      .toLocalIterator.toSeq
    updateDictFile("adslot.txt", lastAdslot)
    println("adslot dict done")

    val lastCity = ulog.map(x => (x.city, 1))
      .reduceByKey((x, y) => x)
      .map(_._1.toString)
      .filter(_.length > 0)
      .toLocalIterator.toSeq
    updateDictFile("city.txt", lastCity)
    println("city dict done")

    val lastChannel = ulog.map(x => (x.ext.getOrElse("channel", ExtValue()).string_value, 1))
      .reduceByKey((x, y) => x)
      .map(_._1)
      .filter(_.length > 0)
      .toLocalIterator.toSeq
    updateDictFile("channel.txt", lastChannel)
    println("channel dict done")

    val lastAdclass = ulog.map(x => (x.ext.getOrElse("adclass", ExtValue()).int_value, 1))
      .reduceByKey((x, y) => x)
      .map(_._1.toString)
      .filter(_.length > 0)
      .toLocalIterator.toSeq
    updateDictFile("adclass.txt", lastAdclass)
    println("adclass dict done")

    /*
    val lastInterest = ulog
      .flatMap {
        x =>
          x.interests.split(",").map(x => (x.split("=")(0), 1))
      }
      .reduceByKey((x, y) => x)
      .map(_._1.toString)
      .filter(_.length > 0)
      .toLocalIterator.toSeq
    updateDictFile("interest.txt", lastInterest)
    println("interest dict done")
    */
  }

  def loadData(): Unit = {
    var n = 0
    loadDictFile("adslot.txt").foreach {
      v =>
        adslot.update(v.toInt, n)
        dict.adslot = adslot.toMap
        n = n + 1
    }

    n = 0
    loadDictFile("city.txt").foreach {
      v =>
        city.update(v.toInt, n)
        dict.city = city.toMap
        n = n + 1
    }

    n = 0
    loadDictFile("channel.txt").foreach {
      v =>
        channel.update(v.toInt, n)
        dict.channel = channel.toMap
        n = n + 1
    }

    n = 0
    loadDictFile("adclass.txt").foreach {
      v =>
        adclass.update(v.toInt, n)
        dict.adclass = adclass.toMap
        n = n + 1
    }

    n = 0
    loadDictFile("interest.txt").foreach {
      v =>
        interest.update(v.toInt, n)
        dict.interest = interest.toMap
        n = n + 1
    }
  }

  private def loadDictFile(filename: String): Seq[String] = {
    val file = "/data/cpc/anal/conf/mldict/" + filename
    Source.fromFile(file, "UTF-8").getLines().filter(_.length > 0).toSeq
  }

  def updateServerData(conf: Config): Unit = {
    val nodes = conf.getStringList("mlserver.nodes")
    for (i <- 0 until nodes.size()) {
      val node = nodes.get(i)
      val ret = s"scp -r /data/cpc/anal/conf/mldict work@$node:/home/work/ml/conf/" !
    }
  }

  private def updateDictFile(filename: String, last: Seq[String]): Unit = {
    val file = "/data/cpc/anal/conf/mldict/" + filename
    val old = Source.fromFile(file, "UTF-8").getLines().filter(_.length > 0).toSeq
    val added = last.diff(old).sortWith(_ < _)
    val w = new PrintWriter(file)
    (old ++ added).foreach {
      v =>
        w.write(v + "\n")
    }
    w.close()
  }

  def saveLua(): Unit = {
    val dir = "/data/cpc/anal/conf/mldict/"
    val w = new PrintWriter(dir + "dict.lua")
    var buffer = Seq[String]()
    buffer = buffer :+ saveLuaDict("interest")
    buffer = buffer :+ saveLuaDict("adclass")
    buffer = buffer :+ saveLuaDict("adslot")
    buffer = buffer :+ saveLuaDict("channel")
    buffer = buffer :+ saveLuaDict("city")
    w.write(buffer.mkString("\n\n"))
    w.close()
  }

  private def saveLuaDict(name: String): String = {
    var n = 0
    val txt = loadDictFile(name + ".txt")
      .map{
        v =>
          n = n + 1
          val s = "%s[%s] = %d".format(name, v, n)
          s
      }
      .mkString("\n")
    "%s = {}\n%s".format(name, txt)
  }
}

case class Dict(var adslot: Map[Int, Int] = null,
                 var adclass: Map[Int, Int] = null,
                 var city: Map[Int, Int] = null,
                 var channel: Map[Int, Int] = null,
                 var interest: Map[Int, Int] = null)





