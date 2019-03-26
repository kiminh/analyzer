package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object Lab {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("appInstallation").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    import spark.implicits._


    val appCat = getAppCat(spark)
    println("============================== appCat ===================================")
    appCat.show(10)

    val appFreq = getAppFreq(spark, date)
    println("============================== appFreq  ======================================")
    appFreq.show(10)

    val app = appCat
      .join(appFreq, Seq("appName"), "left")
      .select("cat", "appName", "appName1", "appName0", "count")
      .filter("cat is not NULL").toDF()

//    app.write.mode("overwrite").saveAsTable("test.AppCat1_sjq")
    println("==============================  app  ==========================================")
    app.show(10)

    val uidApp = getUidApp(spark, date).cache() //uid,pkgs

    uidApp.write.mode("overwrite").saveAsTable("test.uidApp_sjq")

//    val uidApp1 = uidApp
//    uidApp1.show( 10 )

    val uidApp2 = uidApp.rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs1").split(",") ) )
      .map( x => x._2.map( y => UidApp( x._1, y )) ).reduce((x, y) => x ++ y ).toList.toDF()

    uidApp2.write.mode("overwrite").saveAsTable("test.uidApp2_sjq")





  }

  def getAppCat(spark: SparkSession) = {
    import spark.implicits._
    val social = List("挖客", "MOMO陌陌", "比邻", "探探", "MOMO约", "富聊", "抱抱", "UKI", "漂流瓶子", "草莓聊天交友", "百合婚恋", "米聊", "约爱吧", "95爱播", "配配", "桃花洞", "微光", "快猫", "默默聊", "Blued", "花田",
      "附近语聊约会", "同城爱约", "摇一摇交友", "探约探爱-同城交友约会", "甜友聊天交友", "遇到视频聊天", "雨音-一对一视频", "脉脉", "小恩爱", "啵啵", "聊聊", "在哪", "蜜聊", "语玩语音聊天交友约会", "妇聊", "美聊", "tataUFO",
      "玩洽", "陌声同城聊天交友", "富聊一对一视频", "黄瓜视频", "美丽约", "陌聊（陌陌聊天交友）", "碰碰交友", "随缘漂流瓶", "乐聊", "知页Pick", "同城追爱", "甜逗")
    val live = List("火山小视频", "映客", "花椒直播", "石榴直播", "斗鱼直播", "水多直播", "丁香直播", "盒子直播", "深入直播", "一直播", "番茄直播", "快手美女秀", "香蕉直播", "妖娆直播", "小宝贝直播", "易直播", "猫咪视频直播", "快猫直播", "蜜秀直播",
      "快狐直播", "么么直播", "直播吧", "辣舞直播", "大秀直播", "樱桃直播", "浴火直播", "诱火", "嗨秀秀场", "哇塞直播", "小蛮腰直播", "蜜聊直播", "蜜疯直播", "棉花糖", "陌秀直播", "NOW直播", "夜嗨直播", "蜜兔直播", "花间娱乐美女视频直播交友", "水滴直播",
      "要播直播", "伊人直播", "NN直播", "红人直播", "Z直播", "比心直播", "来疯直播", "酷咪直播", "九秀美女直播")
    val shortVideo = List("西瓜视频", "火山小视频", "抖音短视频", "好看视频", "土豆视频", "秒拍", "LIKE短视频", "全民小视频",
      "姜饼短视频", "前排视频", "快手", "全民短视频", "微视", "美拍", "梨视频", "Yoo视频", "百思不得姐", "娃趣视频")
    val lb = scala.collection.mutable.ListBuffer[AppCat]()
    for (app <- social) {
      lb += AppCat(app, "社交")
    }
    for (app <- live) {
      lb += AppCat(app, "直播")
    }
    for (app <- shortVideo) {
      lb += AppCat(app, "短视频")
    }
    lb.toDF()
  }

  def getAppFreq(spark: SparkSession, date: String) = {
    import spark.implicits._
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

    val result = pkgs.rdd
      .map(x => x.getAs[String]("pkgs1"))
      .flatMap(x => x.split(","))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, x._1.split("-"), x._2))
      .map(x =>
        if (x._2.length > 1) {
          AppCount(x._1, x._2(0), x._2(1), x._3)
        } else {
          AppCount(x._1, x._2(0), "", x._3)
        }
      ).toDF()
    result
  }

  def getUidApp(spark: SparkSession, date: String) = {
    val sql =
      s"""
         | select
         |  t1.uid,
         |  t2.pkgs1
         |from (
         | select
         |  uid
         | from dl_cpc.slim_union_log
         |where dt = '$date'
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  and antispam = 0
         |  and media_appsid = '80002819'
         | group by uid ) t1
         | join (
         |   select uid, concat_ws(',', app_name) as pkgs1  from dl_cpc.cpc_user_installed_apps where load_date = '$date' group by uid, app_name
         | ) t2
         | on t1.uid = t2.uid
       """.stripMargin
    val df = spark.sql(sql)
    df
  }

  case class AppCat(var appName: String, var cat: String)
  case class AppCount(var appName0: String, var appName1: String, appName: String, var count: Int)
  case class UidApp( var uid: String, var app: String)

}








