package com.cpc.spark.ml.common

import com.cpc.spark.log.parser.TraceLog
import com.typesafe.config.Config

import scala.collection.mutable
import sys.process._

/**
  * Created by roydong on 23/06/2017.
  */
object Utils {

  /*
  返回组合特征的位置，和最大位置号
   */
  def combineIntFeatureIdx(ids: Int*): Int = {
    var idx = 0
    for (i <- 0 until ids.length) {
      var v = 1
      for (j <- i + 1 until ids.length) {
        v = v * ids(j)
      }
      idx = idx + (ids(i) - 1) * v
      println(idx)
    }
    idx
  }

  def combineIntFeatureMax(m: Int*): Int = {
    var max = 1
    for (i <- 0 until m.length) {
      max = max * m(i)
    }
    max
  }

  def updateOnlineData(srcfile: String, destfile: String, conf: Config): String = {
    val nodes = conf.getConfigList("mlserver.nodes")
    var log = Seq[String]()
    for (i <- 0 until nodes.size()) {
      val node = nodes.get(i)
      var name = node.getString("name")
      val ip = node.getString("ip")
      val cmd = s"scp $srcfile work@$ip:/home/work/ml/model/$destfile"
      log = log :+ "%s %s".format(name, ip)
      val ret = cmd !
    }
    log.mkString("\n")
  }
  def updateAntispamOnlineData(srcfile: String, destfile: String, conf: Config): String = {
    val nodes = conf.getConfigList("mlserver.nodes")
    var log = Seq[String]()
    for (i <- 0 until nodes.size()) {
      val node = nodes.get(i)
      var name = node.getString("name")
      val ip = node.getString("ip")
      val cmd = s"scp $srcfile work@$ip:/home/work/antispamml/model/$destfile"
      log = log :+ "%s %s".format(name, ip)
      val ret = cmd !
    }
    log.mkString("\n")
  }

  def cvrPositive(traces: Seq[TraceLog]): Int = {
    var stay = 0
    var click = 0
    var active = 0
    var mclick = 0
    var zombie = 0
    var disactive = 0
    traces.foreach {
      t =>
        t.trace_type match {
          case s if s.startsWith("active") => active += 1

          case "disactive" => disactive += 1

          case "buttonClick" => click += 1

          case "clickMonitor" => mclick += 1

          case "inputFocus" => click += 1

          case "press" => click += 1

          case "zombie" => zombie += 1

          case "stay" =>
            if (t.duration > stay) {
              stay = t.duration
            }

          case _ =>
        }
    }

    if (((stay >= 30 && click > 0) || active > 0) && disactive == 0) {
      1
    } else {
      0
    }
  }
}

