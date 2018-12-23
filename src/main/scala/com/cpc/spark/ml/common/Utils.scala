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

  def updateMlcppModelData(srcfile: String, destfile: String, conf: Config): String = {
    val nodes = conf.getConfigList("mlserver.model_server")
    var log = Seq[String]()
    for (i <- 0 until nodes.size()) {
      val node = nodes.get(i)
      var name = node.getString("name")
      val ip = node.getString("ip")
      val cmd = s"scp $srcfile cpc@$ip:$destfile"
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
        if (!t.isNullAt(0)) { //trace_type为null时过滤
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

    }

    traces.foreach {
      t =>
        if (!t.isNullAt(1)) { //trace_op1为null时过滤
          t.getAs[String]("trace_op1") match {
            case "REPORT_DOWNLOAD_INSTALLED" => installed += 1

            case _ =>
          }
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

  /* 新cvr计算逻辑 */
  def cvrPositiveV2(traces: Seq[Row], version: String): (Int, Int) = {
    var active5 = 0
    var active3 = 0
    var nosite_active5 = 0
    var disactive = 0
    var active_href = 0

    var nosite_active = 0
    var nosite_disactive = 0

    var conversion_sdk_wechat = 0
    var conversion_sdk_download = 0
    var js_site_active_other = 0
    var js_site_active_other_test = 0

    var label_type = 0 //广告类型，区分不同类型广告

    var active_sdk_site_wz = 0 //加粉类：建站&sdk
    var active_js_site_wz = 0 //加粉类：建站&非sdk
    var active_js_nonsite_wz = 0 //加粉类：非建站
    var active_js_download = 0 //直接下载类
    var active_js_ldy_download = 0 //落地页下载类
    var active_other_site = 0 //其他类建站
    var active_other_nonsite = 0 //其他类非建站


    traces.foreach {
      r =>
        if ((!r.isNullAt(0)) && (!r.isNullAt(1))) { //trace_type和trace_op1为null时过滤
          r.getAs[String]("trace_type") match {
            case "active5" => active5 += 1
            case "active3" => active3 += 1
            case "disactive" => disactive += 1
            case "active_href" => active_href += 1
            case "nosite_disactive" => nosite_disactive += 1
            case "nosite_active5" => nosite_active5 += 1
            case _ =>
          }


          //加粉类：建站&sdk
          if (r.getAs[String]("trace_op1").toLowerCase == "report_user_stayinwx") {
            conversion_sdk_wechat += 1
          }

          //直接下载类、落地页下载类
          if (r.getAs[String]("trace_op1").toLowerCase == "report_download_pkgadded") {
            conversion_sdk_download += 1
          }

          //落地页套户、其他类非建站测试
          if (r.getAs[String]("trace_op1").toLowerCase == "report_download_installed" ||
            (r.getAs[String]("trace_type").startsWith("active") && (r.getAs[String]("trace_type") != "active5"))) {
            js_site_active_other_test += 1
          }

          //其它类：建站
          if (r.getAs[String]("trace_type") == "active1" || r.getAs[String]("trace_type") == "active2" ||
            r.getAs[String]("trace_type") == "active3" || r.getAs[String]("trace_type") == "active4") {
            js_site_active_other += 1
          }

          //其它类：非建站
          if (r.getAs[String]("trace_type").startsWith("nosite_active") && (r.getAs[String]("trace_type") != "nosite_active5")) {
            nosite_active += 1
          }

        }
    }

    traces.foreach {
      r =>
        val adsrc = r.getAs[Int]("adsrc")
        val adclass = r.getAs[Int]("adclass")
        val siteid = r.getAs[Long]("siteid")
        val adslot_type = r.getAs[Int]("adslot_type")
        val client_type = r.getAs[String]("client_type")
        val interaction = r.getAs[Int]("interaction")


        //判断广告类型
        //第一类：建站&sdk：列表页、详情等sdk栏位,网赚+彩票 ：trace_op1 = “REPORT_USER_STAYINWX”
        //第二类：建站&非sdk：详情页、互动位等其他非sdk栏位, 网赚+彩票 ：trace_type: active5-disactive
        //第三类：非建站：对于所有类型(js+sdk+openapi), 3个栏位(表页、详情、互动), 网赚+彩票：trace_type = “active_href”
        //第四类：直接下载类：interaction=2 +Sdk栏位(列表+详情): trace_op1 = “REPORT_DOWNLOAD_PKGADDED”
        //第五类：落地页下载：interaction=1 +Sdk栏位(列表+详情):trace_op1 = “REPORT_DOWNLOAD_PKGADDED”
        //第六、七类：其他类（落地页非下载非加粉类）
        //建站：其他(非网赚非彩票非直接下载类)+所有类型(js+sdk+openapi)+所有栏位,即针对非以上1-4的情况的search_id/click 判断: trace_type: active1/active2/active3/active4/active5-disactive
        //非建站：nosite_active1/nosite_active2/nosite_active3/nosite_active4/nosite_active5-nosite_disactive
        //        if ((adsrc == 0 || adsrc == 1) && (adclass == 110110100 || adclass == 125100100) && siteid > 0 && ((adslot_type == 1 || adslot_type == 2) && client_type == "NATIVESDK")) {
        //          label_type = 1
        //        } else if ((adsrc == 0 || adsrc == 1) && (adclass == 110110100 || adclass == 125100100) && siteid > 0 && (adslot_type == 2 || adslot_type == 3) && client_type != "NATIVESDK") {
        //          label_type = 2
        //        } else if ((adsrc == 0 || adsrc == 1) && (adclass == 110110100 || adclass == 125100100) && siteid <= 0 && (adslot_type == 1 || adslot_type == 2 || adslot_type == 3)) {
        //          label_type = 3
        //        } else if ((adsrc == 0 || adsrc == 1) && interaction == 2 && ((adslot_type == 1 || adslot_type == 2) && client_type == "NATIVESDK")) {
        //          label_type = 4
        //        } else if ((adsrc == 0 || adsrc == 1) && interaction == 1 && (adclass.toString.substring(0, 3).toInt == 100) && ((adslot_type == 1 || adslot_type == 2) && client_type == "NATIVESDK")) {
        //          label_type = 5
        //        } else {
        //          if (siteid > 0) {
        //            label_type = 6 //其它类建站
        //          } else {
        //            label_type = 7 //其它类非建站
        //          }
        //        }

        if ((adclass == 110110100 || adclass == 125100100) && siteid > 0 && client_type == "NATIVESDK") {
          label_type = 1
          if (conversion_sdk_wechat > 0) {
            active_sdk_site_wz += 1
          }
        } else if ((adclass == 110110100 || adclass == 125100100) && siteid > 0 && client_type != "NATIVESDK") {
          label_type = 2
          if (active5 > 0 && disactive == 0) {
            active_js_site_wz += 1
          }
        } else if ((adclass == 110110100 || adclass == 125100100) && siteid <= 0) {
          label_type = 3
          if (active_href > 0) {
            active_js_nonsite_wz += 1
          }
        } else if (interaction == 2 && client_type == "NATIVESDK") {
          label_type = 4
          if (conversion_sdk_download > 0) {
            active_js_download += 1
          }
        }
        // 直接下载非sdk
        else if (interaction == 2 && client_type != "NATIVESDK") {
          label_type = 8
        }

        // 非直接下载 sdk
        else if ((adclass.toString.length > 3 && adclass.toString.substring(0, 3).toInt == 100) && (client_type == "NATIVESDK" || client_type == "JSSDK")) {
          label_type = 5
          if (conversion_sdk_download > 0) {
            active_js_ldy_download += 1
          } else if (siteid <= 0 && active3 > 0) {
            // 测试
            active_other_site += 1
            label_type = 9
          } else if (js_site_active_other_test > 0 || (active5 > 0 && disactive == 0) || nosite_active > 0 || (nosite_active5 > 0 && nosite_disactive == 0)) {
            // 套户
            active_other_site += 1
            label_type = 10
          }
        }


        // review， 落地页下载非sdk，其它
        else {
          if (siteid > 0) {
            label_type = 6 //其它类建站
            if (js_site_active_other > 0 || (active5 > 0 && disactive == 0)) {
              active_other_site += 1
            }
          } else {
            label_type = 7 //其它类非建站
            if (siteid <= 0 && active3 > 0) {
              // 测试
              active_other_site += 1
              label_type = 11
            } else if (nosite_active > 0 || (nosite_active5 > 0 && nosite_disactive == 0) || js_site_active_other_test > 0 || (active5 > 0 && disactive == 0)) {
              active_other_nonsite += 1
            }
          }
        }

      //        if (label_type == 1 && conversion_sdk_wechat > 0) {
      //          active_sdk_site_wz += 1
      //        } else if (label_type == 2 && active5 > 0 && disactive == 0) {
      //          active_js_site_wz += 1
      //        } else if (label_type == 3 && active_href > 0) {
      //          active_js_nonsite_wz += 1
      //        } else if (label_type == 4 && conversion_sdk_download > 0) {
      //          active_js_download += 1
      //        } else if (label_type == 5 && conversion_sdk_download > 0) {
      //          active_js_ldy_download += 1
      //        } else if (label_type == 6 && js_site_active_other > 0 && disactive == 0) {
      //          active_other_site += 1
      //        } else if (label_type == 7 && nosite_active > 0 && nosite_disactive == 0) {
      //          active_other_nonsite += 1
      //        }
    }


    if (active_sdk_site_wz > 0 || active_js_site_wz > 0 || active_js_nonsite_wz > 0 || active_js_download > 0
      || active_js_ldy_download > 0 || active_other_site > 0 || active_other_nonsite > 0) {
      (1, label_type) //1表示转化，0表示未转化；label_type: 广告类型
    } else {
      (0, label_type)
    }


  }

  /**
    * sdk栏位下载app的转化数
    *   1. interaction=2& Sdk栏位(sdk_natvie):
    * 转化数：trace_op1 = “INCITE_OPEN_SUCCESS”
    *   2.一级行业为应用下载(100)&SDK栏位：
    * trace_op1 = “INCITE_OPEN_SUCCESS”
    *
    * @param traces
    * @param version
    * @return
    */
  def cvrPositive_sdk_dlapp(traces: Seq[Row], version: String): Int = {

    var sdk_dlapp = 0
    var sdk_dlapp_active = 0

    traces.foreach {
      r =>
        if ((!r.isNullAt(0)) && (!r.isNullAt(1))) {
          val adclass = r.getAs[Int]("adclass")
          val client_type = r.getAs[String]("client_type")
          val interaction = r.getAs[Int]("interaction")

          if ((interaction == 2 && client_type == "NATIVESDK") || ((adclass.toString.length > 3 && adclass.toString.substring(0, 3).toInt == 100) && (client_type == "NATIVESDK"))) {
            sdk_dlapp = 1
          }

          var trace_op1 = r.getAs[String]("trace_op1")
          if (sdk_dlapp > 0 && trace_op1 == "INCITE_OPEN_SUCCESS") {
            sdk_dlapp_active = 1
          }
        }
    }

    if (sdk_dlapp_active > 0) {
      1
    } else {
      0
    }


  }

  /* 激励下载转化 */
  def cvrPositive_motivate(traces: Seq[Row], version: String): (Int, Int) = {
    var label_type = 0
    var active_motivate = 0

    traces.foreach {
      r =>
        if (r.getAs[String]("trace_type") == "sdk_incite" && r.getAs[String]("trace_op1").toLowerCase == "open_app") {
          active_motivate += 1
        }
    }

    if (active_motivate > 0) {
      (1, 12)
    } else {
      (0, 12)
    }

  }

  /* Api回传转化 */
  def cvrPositive_api(traces: Seq[Row], version: String): (Int, Int) = {
    var label_type = 0
    var active_api = 0

    traces.foreach {
      r =>
        if (r.getAs[String]("trace_type") == "active_third") {
          active_api += 1
        }
    }
    if (active_api > 0) {
      (1, 13)
    } else {
      (0, 13)
    }
  }

}

