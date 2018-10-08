package com.cpc.spark.ml.common

import com.cpc.spark.log.parser.{ExtValue, TraceLog}
import com.typesafe.config.Config
import org.apache.spark.sql.Row

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

  def updateMlcppOnlineData(srcfile: String, destfile: String, conf: Config): String = {
    val nodes = conf.getConfigList("mlserver.model_nodes")
    var log = Seq[String]()
    for (i <- 0 until nodes.size()) {
      val node = nodes.get(i)
      var name = node.getString("name")
      val ip = node.getString("ip")
      val cmd = s"scp $srcfile work@$ip:$destfile"
      log = log :+ cmd
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


  def cvrPositiveV(traces: Seq[Row], version: String): Int = {
    var stay = 0
    var click = 0
    var active = 0
    var mclick = 0
    var zombie = 0
    var disactive = 0
    var installed = 0
    traces.foreach {
      t =>
        t.getAs[String]("trace_type") match {
          case s if s.startsWith("active") => active += 1

          case "disactive" => disactive += 1

          case "buttonClick" => click += 1

          case "clickMonitor" => mclick += 1

          //case "inputFocus" => click += 1

          case "press" => click += 1

          case "zombie" => zombie += 1

          case "stay" =>
            if (t.getAs[Int]("duration") > stay) {
              stay = t.getAs[Int]("duration")
            }

          case _ =>
        }
    }

    traces.foreach {
      t =>
        t.getAs[String]("trace_op1") match {
          case "REPORT_DOWNLOAD_INSTALLED" => installed += 1

          case _ =>
        }
    }


    if (version == "v1") {
      if (((stay >= 30 && click > 0) || active > 0) && disactive == 0) {
        1
      } else {
        0
      }
    }
    else {
      if ((installed > 0 || active > 0) && disactive == 0) {
        1
      } else {
        0
      }
    }
  }

  def cvrPositiveV2(traces: Seq[Row], version: String): (Int, Int) = {

    var report_user_stayinwx = 0
    var active5 = 0
    var disactive = 0
    var active_href = 0
    var report_download_pkgadded = 0
    var active = 0
    var installed = 0
    var label_type = 0 //类型，区分建站、sdk、js、下载类、非网赚非下载类


    var active_sdk_site_wz = 0 //建站sdk栏位网赚
    var active_js_site_wz = 0 //建站非sdk栏位网赚
    var active_js_nonsite_wz = 0 //非建站
    var active_js_download = 0 //下载类
    var other = 0 //非网赚非彩票非下载

    traces.foreach {
      r =>
        val adsrc = r.getAs[Int]("adsrc")
        val adclass = r.getAs[Int]("adclass")
        val siteid = r.getAs[Long]("siteid")
        val adslot_type = r.getAs[Int]("adslot_type")
        val client_type = r.getAs[String]("client_type")
        val interaction = r.getAs[Int]("interaction")


        r.getAs[String]("trace_op1").toLowerCase match {
          case "report_user_stayinwx" => report_user_stayinwx += 1
          case "report_download_pkgadded" => report_download_pkgadded += 1
          case "report_download_installed" => installed += 1
          case _ =>
        }

        r.getAs[String]("trace_type") match {
          case "active5" => active5 += 1
          case "disactive" => disactive += 1
          case "active_href" => active_href += 1
          case s if s.startsWith("active") => active += 1
          case _ =>
        }


        //第一类：建站：详情页、列表页等sdk栏位，网赚+彩票
        //第二类：详情页、互动等其他非sdk栏位(js)，网赚+彩票
        //第三类：所有类型，3个栏位，网赚+彩票
        //第四类：下载类：interation=2+sdk栏位（列表+详情）
        //第五类：其他(非网赚非下载类)+所有类型+所有栏位
        if ((adsrc == 0 || adsrc == 1) && (adclass == 110110100 || adclass == 125100100) && siteid > 0 && ((adslot_type == 1 || adslot_type == 2) && client_type == "NATIVESDK")) {
          label_type = 1
        } else if ((adsrc == 0 || adsrc == 1) && (adclass == 110110100 || adclass == 125100100) && siteid > 0 && (adslot_type == 2 || adslot_type == 3) && client_type != "NATIVESDK") {
          label_type = 2
        } else if ((adsrc == 0 || adsrc == 1) && (adclass == 110110100 || adclass == 125100100) && siteid <= 0 && (adslot_type == 1 || adslot_type == 2 || adslot_type == 3)) {
          label_type = 3
        } else if ((adsrc == 0 || adsrc == 1) && interaction == 2 && ((adslot_type == 1 || adslot_type == 2) && client_type == "NATIVESDK")) {
          label_type = 4
        } else {
          label_type = 5
        }

        if (label_type == 1 && report_user_stayinwx > 0) {
          active_sdk_site_wz += 1
        } else if (label_type == 2 && (active5 > 0 && disactive == 0)) {
          active_js_site_wz += 1
        } else if (label_type == 3 && active_href > 0) {
          active_js_nonsite_wz += 1
        } else if (label_type == 4 && report_download_pkgadded > 0) {
          active_js_download += 1
        } else if (label_type == 5 && ((installed > 0 || active > 0) && disactive == 0)) {
          other += 1
        }
    }


    if (active_sdk_site_wz > 0 || active_js_site_wz > 0 || active_js_nonsite_wz > 0 || active_js_download > 0 || other > 0) {
      (1, label_type)
    } else {
      (0, label_type)
    }


  }


}

