package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("appInstallation").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    import spark.implicits._

    val app_cat = spark.read.csv("hdfs://emr-cluster/warehouse/sunjianqiang/app_cat.csv")
    app_cat.show(10)

    val social = List( "挖客", "MOMO陌陌", "比邻",  "探探", "MOMO约", "富聊",  "抱抱",  "UKI",  "漂流瓶子", "草莓聊天交友",
      "百合婚恋",  "米聊",  "约爱吧",  "95爱播",  "配配",  "桃花洞",  "微光",  "快猫",  "默默聊",  "Blued", "花田",
      "附近语聊约会", "同城爱约", "摇一摇交友",  "探约探爱-同城交友约会", "甜友聊天交友",  "遇到视频聊天", "雨音-一对一视频",
      "脉脉", "小恩爱",  "啵啵",  "聊聊",  "在哪",  "蜜聊", "语玩语音聊天交友约会",  "妇聊",  "美聊", "tataUFO",
      "玩洽",   "陌声同城聊天交友", "富聊一对一视频",  "黄瓜视频",  "美丽约",  "陌聊（陌陌聊天交友）",  "碰碰交友",
      "随缘漂流瓶",  "乐聊", "知页Pick",  "同城追爱", "甜逗" )

    val lb = scala.collection.mutable.ListBuffer[AppCat]()

    for (app <- social){
      lb += AppCat(app, "社交")
    }

    lb.toList.toDF().write.mode("overwrite").saveAsTable("test.AppCat1_sjq")




    val sql1 =
      s"""
         |select
         | uid,
         | concat_ws(',', app_name) as pkgs1
         |from dl_cpc.cpc_user_installed_apps a
         |where load_date = '$date'
       """.stripMargin
    val pkgs = spark.sql(sql1)
    pkgs.show(3)

    val appFreq = pkgs.rdd
      .map(x =>  x.getAs[String]("pkgs1") )
      .flatMap(x => x.split(","))
      .map(x => (x,1)).reduceByKey((x, y) => x+y).map(x => (x._1.split("-"), x._2)).map( x => if(x._1.length > 1) AppCount(x._1(0), x._1(1), x._2) else AppCount(x._1(0), "" , x._2)).toDF()

    appFreq.show(10)

    appFreq.write.mode("overwrite").saveAsTable("test.appInstalledCount_sjq")

//    appFreq.orderBy( appFreq("count").desc ).show(50, false)

  }

  case class AppCat( var appName: String, var cat: String)
  case class AppCount( var appName1: String, appName2: String, var count: Int)
}
