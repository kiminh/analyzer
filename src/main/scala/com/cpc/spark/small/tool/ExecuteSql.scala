package com.cpc.spark.small.tool

import com.cpc.spark.log.parser.LogParser
import com.cpc.spark.small.tool.InsertReportSdkTrace.Info
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Success

/**
  * Created by wanli on 2017/9/5.
  */
object ExecuteSql {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)
    val ctx = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", "800")
      .config("spark.driver.maxResultSize", "10G")
      .appName("Let me fly")
      .enableHiveSupport()
      .getOrCreate()

    val unionData = ctx
      .sql(
        """
          |SELECT sum(isshow),sum(isclick)
          |FROM dl_cpc.cpc_union_log
          |WHERE date="2018-08-04" AND interests like "%225=%" and userid=1529810
        """.stripMargin)
      .rdd
    .take(100)
    .foreach(println)

//        ctx.sql(
//          """
//            |select
//            |case when(click/read_pv_inner)>1 then ">1"
//            |    when 0.5<(click/read_pv_inner) and (click/read_pv_inner)<=1 then "0.5<a<=1"
//            |    when 0.3<(click/read_pv_inner) and (click/read_pv_inner)<=0.5 then "0.3<a<=0.5"
//            |    else "<=0.3" end,
//            |    count(DISTINCT(device)),
//            |    sum(cv_list_sdk_site_wz),sum(click_list_sdk_site_wz),
//            |    sum(cv_list_sdk_site_wz)/sum(click_list_sdk_site_wz)
//            |from bdm.qukan_daily_active_extend_p_all_bydevice
//            |where new_old_group in("(1w,2w]") and day="2018-08-01"
//            |group by
//            |case
//            |    when(click/read_pv_inner)>1 then ">1"
//            |    when 0.5<(click/read_pv_inner) and (click/read_pv_inner)<=1 then "0.5<a<=1"
//            |    when 0.3<(click/read_pv_inner) and (click/read_pv_inner)<=0.5 then "0.3<a<=0.5"
//            |    else "<=0.3" end
//          """.stripMargin)
//    .rdd
//    .take(100)
//    .foreach(println)

//    //hd_
//    val allData = ctx.sql(
//      """
//        |SELECT date,hour,count(*)
//        |FROM dl_cpc.cpc_all_trace_log
//        |WHERE date in("%s","%s") AND trace_type LIKE "%s" AND trace_op1="7412553"
//        |GROUP BY date,hour
//      """.stripMargin.format("2018-07-23", "2018-07-24","hd_load_%"))
//      .rdd
//      .map {
//        x =>
//          var date = x.get(0).toString
//          var hour = x.get(1).toString.toLong
//          var total = x.get(2).toString.toLong
//          (date,hour, total)
//      }
//      .cache()
//
//    println("allData count is", allData.count())
//    allData.take(1000).foreach(println)planid,unitid,ideaid,bid,price,country,province,city,network
//    println("------------------------------------2018-07-24")
//    ctx.sql(
//      """
//        |select count(*)
//        |from dl_cpc.cpc_union_log
//        |where adslotid="7453081" and `date`="2018-07-24" and
//        |`timestamp`>=1532433600 and `timestamp`<=1532435700
//      """.stripMargin)
//    .rdd
////    .map{
////      x=>
////        val planid = x.get(0).toString.toLong
////        val unitid = x.get(1).toString.toLong
////        val ideaid = x.get(2).toString.toLong
////        val bid = x.get(3).toString.toLong
////        val price = x.get(4).toString.toLong
////        val country = x.get(5).toString.toLong
////        val province = x.get(6).toString.toLong
////        val city = x.get(7).toString.toLong
////        val network = x.get(8).toString.toLong
////        "%d,%d,%d,%d,%d,%d,%d,%d,%d".format(planid,unitid,ideaid,bid,price,country,province,city,network)
////    }
//    .take(100000)
//    .foreach(println)
//
//    println("------------------------------------2018-07-23")
//    ctx.sql(
//      """
//        |select count(*)
//        |from dl_cpc.cpc_union_log
//        |where adslotid="7453081" and `date`="2018-07-23" and
//        |`timestamp`>=1532347200 and `timestamp`<=1532349300
//      """.stripMargin)
//      .rdd
//      .take(100000)
//      .foreach(println)
//    println("------------------------------------")
//         ctx
//          .sql(
//            """
//              |SELECT DISTINCT cul.searchid,cul.isclick,
//              |FROM dl_cpc.cpc_union_trace_log cutl
//              |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//              |WHERE cutl.date="%s" AND cul.date="%s" AND cul.hour="08" AND cutl.hour="08"
//              |AND cul.adslotid in("7034978") AND cutl.trace_type="load" AND isclick>0 AND
//            """.stripMargin.format("2018-07-24", "2018-07-24"))
//          .rdd
//          .take(100)
//          .foreach(println)
        //println("jsTraceDataByDownSdk3 count is", jsTraceDataByDownSdk3)

    //    val allClickData = ctx.sql(
    //      """
    //        |SELECT DISTINCT cul.searchid,cutl.trace_op1
    //        |from dl_cpc.cpc_union_log cul
    //        |left join dl_cpc.cpc_union_trace_log cutl on cul.searchid = cutl.searchid
    //        |where cul.date = "2018-05-24" and cutl.date = "2018-05-24"
    //        |and cul.ext["client_type"].string_value="NATIVESDK"
    //        |AND cul.interaction=2
    //        |and cul.adslot_type = 1
    //        |and cul.adslotid in (1027423)
    //        |and cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED","REPORT_DOWNLOAD_FINISH")
    //        |and cul.isclick > 0
    //      """.stripMargin)
    //    .rdd
    //    .map{
    //      x=>
    //        val trace_op1 = x.getString(1)
    //        val count = 1.toLong
    //        (trace_op1,(count))
    //    }
    //    .reduceByKey{
    //      (a,b)=>
    //        (a+b)
    //    }

//    val jsTraceDataByDownSdk = ctx
//      .sql(
//        """
//          |SELECT DISTINCT cutl.searchid,cutl.trace_op1
//          |FROM dl_cpc.cpc_union_trace_log cutl
//          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown") AND cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED")
//          |AND cul.ext["client_type"].string_value="NATIVESDK"
//          |AND cul.interaction=2
//          |AND cul.isclick>0 AND cul.adsrc=1
//        """.stripMargin.format("2018-07-05", "2018-07-05"))
//      .rdd
//      .count()
//    println("jsTraceDataByDownSdk count is", jsTraceDataByDownSdk)
//
//
//    val jsTraceDataByDownSdk1 = ctx
//      .sql(
//        """
//          |SELECT DISTINCT cutl.searchid,cutl.trace_op1
//          |FROM dl_cpc.cpc_union_trace_log cutl
//          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown") AND cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED")
//          |-- AND cul.ext["client_type"].string_value="NATIVESDK"
//          |-- AND cul.interaction=2
//          |-- AND cul.isclick>0 AND cul.adsrc=1
//        """.stripMargin.format("2018-07-05", "2018-07-05"))
//      .rdd
//      .count()
//    println("jsTraceDataByDownSdk1 count is", jsTraceDataByDownSdk1)
//
//    val jsTraceDataByDownSdk2 = ctx
//      .sql(
//        """
//          |SELECT DISTINCT cutl.searchid,cutl.trace_op1
//          |FROM dl_cpc.cpc_union_trace_log cutl
//          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown") AND cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED")
//          | AND cul.ext["client_type"].string_value="NATIVESDK"
//          |-- AND cul.interaction=2
//          |-- AND cul.isclick>0 AND cul.adsrc=1
//        """.stripMargin.format("2018-07-05", "2018-07-05"))
//      .rdd
//      .count()
//    println("jsTraceDataByDownSdk2 count is", jsTraceDataByDownSdk2)
//
//    val jsTraceDataByDownSdk3 = ctx
//      .sql(
//        """
//          |SELECT DISTINCT cutl.searchid,cutl.trace_op1
//          |FROM dl_cpc.cpc_union_trace_log cutl
//          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown") AND cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED")
//          | AND cul.ext["client_type"].string_value="NATIVESDK"
//          | AND cul.interaction=2
//          |-- AND cul.isclick>0 AND cul.adsrc=1
//        """.stripMargin.format("2018-07-05", "2018-07-05"))
//      .rdd
//      .count()
//    println("jsTraceDataByDownSdk3 count is", jsTraceDataByDownSdk3)
//
//    val jsTraceDataByDownSdk4 = ctx
//      .sql(
//        """
//          |SELECT DISTINCT cutl.searchid,cutl.trace_op1
//          |FROM dl_cpc.cpc_union_trace_log cutl
//          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown") AND cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED")
//          | AND cul.ext["client_type"].string_value="NATIVESDK"
//          | AND cul.interaction=2
//          |AND cul.isclick>0
//          |-- AND cul.adsrc=1
//        """.stripMargin.format("2018-07-05", "2018-07-05"))
//      .rdd
//      .count()
//    println("jsTraceDataByDownSdk4 count is", jsTraceDataByDownSdk4)
//
//    val jsTraceDataByDownSdk5 = ctx
//      .sql(
//        """
//          |SELECT DISTINCT cutl.searchid,cutl.trace_op1
//          |FROM dl_cpc.cpc_union_trace_log cutl
//          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown") AND cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED")
//          | AND cul.ext["client_type"].string_value="NATIVESDK"
//          | AND cul.interaction=2
//          | AND cul.isclick>0
//          | AND cul.adsrc=1
//        """.stripMargin.format("2018-07-05", "2018-07-05"))
//      .rdd
//      .count()
//    println("jsTraceDataByDownSdk5 count is", jsTraceDataByDownSdk5)

//    val data = ctx.sql(
//      """
//        |SELECT userid,planid,unitid,ideaid,isshow,isclick
//        |FROM dl_cpc.cpc_union_log cul
//        |WHERE cul.`date`="2018-06-21"
//        |AND cul.ext["client_type"].string_value="NATIVESDK" and cul.planid=1586108
//        |AND cul.interaction=2
//      """.stripMargin)
//      .rdd
//
//    println("data",data.count())
//    data.take(1000).foreach(println)
//
//    println("")
//    println("")
//    println("")
//
//    val data1 = ctx.sql(
//      """
//        |SELECT DISTINCT cutl.searchid,
//        |cul.userid,cul.planid,cul.unitid,cul.ideaid,cul.isfill,cul.isshow,cul.isclick,
//        |cutl.trace_type,cutl.trace_op1
//        |FROM dl_cpc.cpc_union_trace_log cutl
//        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
//        |WHERE cutl.date="2018-06-21" AND cul.date="2018-05-21" AND cutl.trace_type in("apkdown")
//        |AND cul.ext["client_type"].string_value="NATIVESDK"
//        |AND cul.interaction=2
//        |AND cul.isclick>0
//      """.stripMargin)
//      .rdd
//
//    println("data1",data1.count())
//    data1.take(1000).foreach(println)


    //    println("")
    //    println("")
    //    println("allData ")
    //    val allData = ctx.sql(
    //      """
    //        |SELECT SUM(isclick)
    //        |from dl_cpc.cpc_union_log cul
    //        |where cul.date ="2018-05-20"
    //        |AND cul.ext["adclass"].int_value=110110100
    //        |AND cul.ext_int["siteid"]>0
    //        |AND cul.media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
    //        |AND cul.adslot_type=1
    //      """.stripMargin)
    //      .rdd
    //      .take(10)
    //      .foreach(println)
    //    val allData = ctx.sql(
    //      """
    //        |SELECT DISTINCT cul.searchid,cutl.trace_op1,cul.ext["client_version"].string_value
    //        |from dl_cpc.cpc_union_log cul
    //        |left join dl_cpc.cpc_union_trace_log cutl on cul.searchid = cutl.searchid
    //        |where cul.date >="2018-05-23" and cutl.date >= "2018-05-23"
    //        |and cul.ext["client_type"].string_value="NATIVESDK"
    //        |AND cul.interaction=2
    //        |and cul.adslotid in (1027423)
    //        |and cutl.trace_op1 in("REPORT_DOWNLOAD_PKGADDED","REPORT_DOWNLOAD_INSTALLED")
    //        |and cul.isclick > 0
    //      """.stripMargin)
    //      .rdd
    //      .map{
    //        x=>
    //          val trace_op1 = x.getString(1)
    //          val count = 1.toLong
    //          val client_version = x.getString(2)
    //          ((client_version,trace_op1),(count))
    //      }
    //      .reduceByKey{
    //        (a,b)=>
    //          (a+b)
    //      }

    //    println("")
    //    println("")
    //    println("allClickData is")
    //    allClickData.take(100).foreach(println)

    //    println("")
    //    println("")
    //    println("allData is")
    //    allData.take(10000).foreach(println)
    //    println("allTraceData count is ")
    //    val allTraceData = ctx.sql(
    //      """
    //        |SELECT date,count(*)
    //        |FROM dl_cpc.cpc_all_trace_log
    //        |WHERE date>="2018-05-08" AND trace_op1 in("REPORT_USER_STAYINWX")
    //        |group by date
    //      """.stripMargin)
    //      .rdd
    //    .take(100)
    //    .foreach(println)

    //    println("traceData count is ")
    //    val traceData = ctx.sql(
    //      """
    //        |SELECT date,count(*)
    //        |FROM dl_cpc.cpc_union_trace_log
    //        |WHERE date>="2018-05-08" AND trace_op1 in ("REPORT_USER_STAYINWX")
    //        |group by date
    //      """.stripMargin)
    //      .rdd
    //      .take(100)
    //      .foreach(println)

    //    ctx.sql(
    //      """
    //        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.trace_op1
    //        |FROM dl_cpc.cpc_union_trace_log cutl
    //        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
    //        |WHERE cutl.date="2018-05-11" AND cul.date="2018-05-11" AND cul.isclick>0 AND cul.ideaid in(1784493,1784507,1784526,1798843,1798846)
    //        |AND cul.userid>0
    //      """.stripMargin)
    //    .rdd
    //      .map{
    //      x=>
    //        val trace_type = x.getString(1)
    //        val trace_op1 = x.getString(2)
    //        ((trace_type,trace_op1),(1))
    //    }
    //    .reduceByKey{
    //      (a,b)=>
    //        (a+b)
    //    }
    //    .take(100000)
    //    .foreach(println)
    //    ctx.sql(
    //      """
    //        |select sum(isshow) imp, count(*) req, ext["adid_str"].string_value adid_str
    //        |from dl_cpc.cpc_union_log
    //        |where adsrc = 6 and `date` = "2018-05-10"
    //        |group by ext["adid_str"].string_value
    //        |order by req desc
    //      """.stripMargin)
    //      .take(10000)
    //      .foreach(println)

    //    val dataAll = ctx.sql(
    //      """
    //        |SELECT date,count(*)
    //        |FROM dl_cpc.cpc_all_trace_log
    //        |WHERE `date`>="2018-05-11" AND trace_type="load_xianwan2"
    //        |group by date
    //      """.stripMargin)
    //      .rdd
    //      .take(10)
    //    println("dataAll count is ")
    //    dataAll.foreach(println)
    //
    //
    //    val dataAll1 = ctx.sql(
    //      """
    //        |SELECT DISTINCT date,trace_op1
    //        |FROM dl_cpc.cpc_all_trace_log
    //        |WHERE `date`>="2018-05-11" AND trace_type="load_xianwan2"
    //      """.stripMargin)
    //      .rdd
    //      .map {
    //        x =>
    //          val date = x.getString(0)
    //          val op1 = x.getString(1)
    //          ((date), (1.toLong))
    //      }
    //      .reduceByKey {
    //        (a, b) =>
    //          (a + b)
    //      }
    //      .map {
    //        x =>
    //          (x._1, x._2)
    //      }
    //      .take(10)
    //
    //    println("dataAll1 DISTINCT count is")
    //    dataAll1.foreach(println)

    //    dataAll
    //      .take(20000)
    //      .foreach(println)

    //    val brandData = ctx.sql(
    //      """
    //        |SELECT brand,count(*)
    //        |from dl_cpc.cpc_union_log cul
    //        |WHERE `date`>= "2018-05-13" AND cul.ext["client_type"].string_value="NATIVESDK" and adslot_type=1 and isfill>0
    //        |GROUP BY brand
    //      """.stripMargin)
    //      .rdd
    //      .map {
    //        x =>
    //          val brand = x.getString(0)
    //          val xx = x.get(1).toString.toLong
    //          (brand, xx)
    //      }
    //      .sortBy(_._2, false)
    //    .take(1000)
    //    .foreach(println)
    //println("brandData count is ",brandData.count())

    //    println("unionLogDataByAllSdk is")
    //    //获取全量sdk流量信息
    //    val unionLogDataByAllSdk = ctx
    //      .sql(
    //        """
    //          |SELECT sum(isshow)
    //          |FROM dl_cpc.cpc_union_log cul
    //          |WHERE cul.date="2018-05-16" AND cul.hour="19" AND cul.adslotid in("1029077")
    //          |-- AND cul.ext["client_type"].string_value="NATIVESDK" AND isshow =1
    //        """.stripMargin)
    //      .rdd
    //    .take(100)
    //    .foreach(println)

    //    println("hd_load is hd_load_")
    //    ctx.sql(
    //      """
    //        |SELECT trace_op1,count(*)
    //        |from dl_cpc.cpc_all_trace_log
    //        |WHERE date="2018-05-17" AND trace_type LIKE "hd_load_%"
    //        |GROUP BY trace_op1
    //      """.stripMargin)
    //    .rdd
    //    .map{
    //      x=>
    //        val trace_op1 = x.getString(0)
    //        val total = x.get(1).toString.toLong
    //        (trace_op1,total)
    //    }
    //    .take(1000)
    //    .foreach(println)
    //    println("brand --------------")
    //    ctx.sql(
    //      """
    //        |SELECT brand,count(*)
    //        |from dl_cpc.cpc_union_log cul
    //        |where date="2018-05-14" AND ideaid in(1759061,1759081,1759103,1759148,1759170,1759193,1759202,1759211,1759229,
    //        |1759238,1759269,1759309,1759379,1760179,1760183,1760212,1781381,1781397,1781416,1781455,1781480,1781497,
    //        |1781513,1781520,1781525,1781530)
    //        | AND media_appsid in ("80000001","80000002") and adslot_type=1
    //        | AND (isshow+isclick)>0
    //        | GROUP BY brand
    //      """.stripMargin)
    //      .rdd
    //      .take(100000)
    //      .foreach(println)
    //    println("browser_type --------------")
    //    ctx.sql(
    //      """
    //        |SELECT ext_int["browser_type"],count(*)
    //        |from dl_cpc.cpc_union_log cul
    //        |where date="2018-05-14" AND ideaid in(1759061,1759081,1759103,1759148,1759170,1759193,1759202,1759211,1759229,
    //        |1759238,1759269,1759309,1759379,1760179,1760183,1760212,
    //        |1781381,1781397,1781416,1781455,1781480,1781497,1781513,1781520,1781525,1781530)
    //        | AND media_appsid not in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
    //        | AND (isshow+isclick)>0
    //        | GROUP BY ext_int["browser_type"]
    //      """.stripMargin)
    //    .rdd
    //    .take(100)
    //    .foreach(println)
    //    val brandData = ctx.sql(
    //      """
    //        |SELECT ext_int["browser_type"],count(*)
    //        |from dl_cpc.cpc_union_log cul
    //        |WHERE `date`>= "2018-05-13" AND media_appsid not in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
    //        |GROUP BY ext_int["browser_type"]
    //      """.stripMargin)
    //      .rdd
    //      .take(1000)
    //      .foreach(println)
    //    ctx.sql(
    //      """
    //        |SELECT cul.ext["adid_str"].string_value,cul.ext["ad_title"].string_value,
    //        |cul.ext["ad_desc"].string_value,cul.ext["ad_img_urls"].string_value,
    //        |cul.ext["ad_click_url"].string_value,isshow,isclick,cul.adsrc
    //        |FROM dl_cpc.cpc_union_log cul
    //        |WHERE cul.`date`="2018-05-07" AND cul.ext["adid_str"].string_value != "" AND cul.adsrc>1
    //        |AND (cul.isclick+cul.isshow)>0
    //        |""".stripMargin)
    //      .rdd
    //      .map {
    //        x =>
    //          val adid_str = x.getString(0)
    //          val ad_title = x.getString(1)
    //          val ad_desc = x.getString(2)
    //          val ad_img_urls = x.getString(3)
    //          val ad_click_url = x.getString(4)
    //          val isshow = x.getInt(5).toLong
    //          val isclick = x.getInt(6).toLong
    //          val src = x.get(7).toString.toInt
    //          //"%s@@@%s@@@%s@@@%s@@@%s".format(adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url)
    //          ((src, adid_str), (src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick))
    //      }
    //      .reduceByKey {
    //        (a, b) =>
    //          val src = a._1
    //          val adid_str = a._2
    //          val ad_title = if (a._3.length > 0) a._3 else b._3
    //          val ad_desc = if (a._3.length > 0) a._4 else b._4
    //          val ad_img_urls = if (a._3.length > 0) a._5 else b._5
    //          val ad_click_url = if (a._3.length > 0) a._6 else b._6
    //          val isshow = a._7 + b._7
    //          val isclick = a._8 + b._8
    //          (src, adid_str, ad_title, ad_desc, ad_img_urls, ad_click_url, isshow, isclick)
    //      }
    //      .sortBy(_._2._7, false)
    //      .map {
    //        x =>
    //          x._2
    //      }
    //      .take(200)
    //      .foreach(println)
    //        val jsTraceDataByDownSdk = ctx
    //          .sql(
    //            """
    //              |SELECT DISTINCT cutl.searchid,cul.adslotid,cutl.trace_type
    //              |FROM dl_cpc.cpc_union_trace_log cutl
    //              |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
    //              |WHERE cutl.`date`="%s" AND cul.`date`="%s" AND cutl.trace_type in("load")
    //              |AND cul.isclick>0 AND cul.userid in(1513381)
    //            """.stripMargin.format("2018-04-04", "2018-04-04"))
    //          .rdd
    //          .map{
    //            x=>
    //              val adslotid = x.getString(1)
    //              val total = 1
    //              (adslotid,(total))
    //          }
    //          .reduceByKey{
    //            (a,b)=>
    //              (a+b)
    //          }
    //          .take(10000)
    //          .foreach(println)

    //        ctx.sql(
    //          """
    //            |SELECT catl.trace_type,catl.trace_op2,catl.trace_op1
    //            |FROM dl_cpc.cpc_all_trace_log catl
    //            |WHERE catl.`date`="2018-04-10" AND catl.hour="12" AND catl.trace_type IS NOT NULL
    //            |AND catl.trace_op1="万人斗地主" AND catl.trace_op2 IS NOT NULL
    //          """.stripMargin).rdd
    //        .map{
    //          x=>
    //           "%s,%s,%s".format(x.get(0).toString,x.get(1).toString,x.get(2).toString)
    //        }
    //        .take(100)
    //        .foreach(println)

    //    ctx.sql(
    //      """
    //        |select ext["phone_level"].int_value,sum(isshow),sum(isclick)
    //        |from dl_cpc.cpc_union_log
    //        |where ext["adclass"].int_value in(100101101,100101113,100101109)  and `date` >'2017-08-15'
    //        |and `date`<'2017-08-23'
    //        |group by ext["phone_level"].int_value
    //      """.stripMargin).rdd
    //        .map{
    //          x=>
    //           "%s,%s,%s".format(x.get(0).toString,x.get(1).toString,x.get(2).toString)
    //        }
    //        .take(200)
    //        .foreach(println)

    //    ctx.sql(
    //      """
    //        |SELECT qkc.device,qkc.member_id
    //        |from rpt_qukan.qukan_log_cmd qkc
    //        |WHERE qkc.cmd=301 AND qkc.thedate>="2017-10-10"  AND qkc.member_id IS NOT NULL
    //        |AND qkc.device IS NOT NULL LIMIT 100
    //      """.stripMargin).rdd
    //      .map {
    //        x =>
    //          "%s,%s".format(x.getString(0),x.get(1).toString)
    //      }
    //      .take(100)
    //      .foreach(println)

    //    //游戏中心
    //    ctx.sql(
    //      """
    //        |SELECT catl.hour,catl.trace_type
    //        |FROM dl_cpc.cpc_all_trace_log catl
    //        |WHERE catl.date="%s" AND catl.trace_type IS NOT NULL
    //      """.stripMargin.format("2018-03-28"))
    //      .rdd
    //      .map {
    //        x =>
    //          val hour = x.getString(0)
    //          val traceType = x.getString(1)
    //          var total = 0
    //          if (traceType.startsWith("load_gameCenter") || traceType.startsWith("active_game")) {
    //            total = 1
    //          }
    //          ((hour, traceType), (traceType, hour, total))
    //      }
    //      .filter(_._2._3 > 0)
    //      .reduceByKey {
    //        (a, b) =>
    //          (a._1, a._2, a._3 + b._3)
    //      }
    //      .map {
    //        x =>
    //          x._2
    //      }
    //      .take(10000)
    //      .foreach(println)

    /**
      * SELECT ext["media_app_version"].string_value,sum(isshow),sum(isclick)
      * FROM cpc_union_log
      * WHERE `date`="2018-03-19"
      * AND media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
      * AND adslot_type=1
      * GROUP BY ext["media_app_version"].string_value
      */
    //    ctx.sql(
    //      """
    //        |SELECT ext["media_app_version"].string_value,sum(isshow),sum(isclick)
    //        |FROM dl_cpc.cpc_union_log
    //        |WHERE `date`="2018-03-19"
    //        |AND media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
    //        |AND adslot_type=1
    //        |GROUP BY ext["media_app_version"].string_value
    //      """.stripMargin)
    //      .rdd
    //      .map {
    //        x =>
    //          val mav = x.getString(0)
    //          val isshow = x.get(1).toString.toLong
    //          val isclick = x.get(2).toString.toLong
    //          (mav,isshow,isclick)
    //      }
    //      .filter {
    //        x =>
    //          val mediaAppVersion = x._1
    //          (mediaAppVersion == "20826" || mediaAppVersion.startsWith("2.8.26"))
    //      }
    //    .take(1000)
    //    .foreach(println)
    //    //site=483
    //    println("searchid,ideaid,media_appsid,adslotid,adslot_type,network,ip,country,province,city,ua,os,brand,model,sex,age,hour, phone_level, os_version, client_version")
    //
    //    ctx.sql(
    //      """
    //        |SELECT cul.searchid,cul.ideaid,cul.media_appsid,cul.adslotid,cul.adslot_type,
    //        |cul.network,cul.ip,cul.country,cul.province,cul.city,
    //        |cul.ua,cul.os,cul.brand,cul.model,cul.sex,cul.age,cul.hour,ext['phone_level'].int_value,
    //        |ext['os_version'].string_value,ext['client_version'].string_value
    //        |FROM dl_cpc.cpc_union_log cul
    //        |WHERE cul.date="%s" AND cul.isclick>0 AND cul.os=1 AND cul.ideaid in(
    //        |1642319,1642322,1642326,1642334,1642340,
    //        |1642342,1642348,1642389,1642400,1651250,
    //        |1694448,1695189,1695211,1695223,1695238,
    //        |1695253,1695297
    //        |)
    //      """.stripMargin.format("2018-04-01"))
    //      .rdd
    //      .take(10000)
    //      .foreach(println)
    ////782.9
    //
    //    println("searchid,load")
    //    //获取sdk下载流量信息
    //    val jsTraceDataByDownSdk = ctx
    //      .sql(
    //        """
    //          |SELECT DISTINCT cutl.searchid,cutl.trace_type
    //          |FROM dl_cpc.cpc_union_trace_log cutl
    //          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
    //          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("load")
    //          |AND cul.isclick>0 AND cul.ideaid in(
    //          |1642319,1642322,1642326,1642334,1642340,
    //          |1642342,1642348,1642389,1642400,1651250,
    //          |1694448,1695189,1695211,1695223,1695238,
    //          |1695253,1695297) AND cul.os=1
    //        """.stripMargin.format("2018-04-01", "2018-04-01"))
    //      .rdd
    //      .take(10000)
    //      .foreach(println)
    //      .map {
    //        x =>
    //          val hour = x.getString(0)
    //          val traceType = x.getString(1)
    //          var total = 0
    //          if (traceType.startsWith("load_gameCenter") || traceType.startsWith("active_game")) {
    //            total = 1
    //          }
    //          (traceType, (traceType, total))
    //      }
    //      .filter(_._2._2 > 0)
    //      .reduceByKey {
    //        (a, b) =>
    //          (a._1, a._2 + b._2)
    //      }
    //      .map {
    //        x =>
    //          x._2
    //      }
    //      .take(10000)
    //      .foreach(println)

    //    //sdk平均请求时间
    //    ctx.sql(
    //      """
    //        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.trace_op1,cutl.trace_op2,cutl.trace_op3
    //        |FROM dl_cpc.cpc_union_trace_log cutl
    //        |WHERE cutl.date="%s" AND trace_type="nsdkdelay"
    //      """.stripMargin.format("2018-03-27"))
    //      .rdd
    //      .map {
    //        x =>
    //          val traceType = x.getString(1)
    //          val traceOp1 = x.getString(2).toLong
    //          val traceOp2 = x.getString(3).toLong
    //          val traceOp3 = x.getString(4).toLong
    //          (traceOp1, traceOp2, traceOp3, 1)
    //      }
    //      .take(100000)
    //      .foreach(println)

    //      .cache()
    //
    //    val reduceData = allData.reduce {
    //      (a, b) =>
    //        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
    //    }
    //    println("reduceData",reduceData)
    //    val allData = ctx.sql(
    //      """
    //        |SELECT scsm.field["cpc_trace"].string_type
    //        |FROM dl_cpc.src_cpc_search_minute scsm
    //        |WHERE scsm.thedate="%s" AND scsm.field["cpc_trace"].string_type is not null limit 1000
    //      """.stripMargin.format("2018-03-27"))
    //      .rdd
    //      .filter{
    //        x=>
    //           x.getString(0).indexOf("nsdkdelay") != -1
    //      }
    //      .map {
    //        x =>
    //          val field = x.getString(0)
    //          val traceOp1 = LogParser.parseTraceLog(field).trace_op1
    //          val traceOp2 = LogParser.parseTraceLog(field).trace_op2
    //          val traceOp3 = LogParser.parseTraceLog(field).trace_op3
    //          (traceOp1, traceOp2, traceOp3, 1)
    //      }
    //      .cache()
    //
    //    println("allData.count()", allData.count())
    //    allData.take(1000).foreach(println)
    //    val dataCount1 = ctx.sql(
    //      """
    //        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration
    //        |FROM dl_cpc.cpc_union_trace_log cutl
    //        |LEFT JOIN  dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
    //        |WHERE cutl.date="%s" AND cul.date="%s" AND cul.isclick>0
    //      """.stripMargin.format("2018-03-25", "2018-03-25"))
    //      .count()
    //    println("count ,count2", dataCount, dataCount1)
    //    val traceDataBySdk = ctx.sql(
    //      """
    //        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration,cutl.trace_op1,cul.adslotid
    //        |FROM dl_cpc.cpc_union_trace_log cutl
    //        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
    //        |WHERE cutl.date="%s" AND cul.date="%s" AND cul.isclick>0 AND cutl.trace_type in("apkdown")
    //        |AND cul.ext["client_type"].string_value="NATIVESDK"
    //        |AND cul.ext["client_version"].string_value="1.29.0.0"
    //      """.stripMargin.format("2018-03-14", "2018-03-14"))
    //      .rdd
    //      .map {
    //        x =>
    //          val searchid = x.getString(0)
    //          val trace_type = if (x.getString(1) == "stay") "%s%d".format(x.getString(1), x.getInt(2)) else x.getString(1)
    //          val duration = x.getInt(2)
    //          //val hour = x.getString(3).toInt
    //          val trace_op1 = x.getString(3)
    //          val adslotid = x.getString(4)
    //          ((adslotid, trace_type, trace_op1), (1))
    //      }
    //      .reduceByKey {
    //        (a, b) =>
    //          val count = a + b
    //          (count)
    //      }
    //      .map {
    //        x =>
    //          //val hour = x._1._1
    //          val adslotid = x._1._1
    //          val trace_type = x._1._2
    //          val trace_op1 = x._1._3
    //          val count = x._2
    //          //          val isfill = 0
    //          //          val isshow = 0
    //          //          val isclick = 0
    //          //          val isreq = 0
    //          //(adslotid, isreq, isfill, isshow, isclick, trace_type, trace_op1, count)
    //          (adslotid, trace_type, trace_op1, count)
    //      }
    //      .repartition(50)
    //      .cache()
    //    println("traceDataBySdk count", traceDataBySdk.count())
    //
    //    traceDataBySdk.take(1000).foreach(println)
  }
}
