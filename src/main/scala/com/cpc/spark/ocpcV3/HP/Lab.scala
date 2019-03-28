package com.cpc.spark.ocpcV3.HP

import com.cpc.spark.qukan.userprofile.SetUserProfileTag.SetUserProfileTagInHiveDaily
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object Lab {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("appInstallation").enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val days = args(1).toInt

    val appCat = getAppCat(spark) // appName, cat
    println("============================== appCat ===================================")
    appCat.show(10)

    val appFreq = getAppFreq(spark, date) // appName0, appName1, appName, count
    println("============================== appFreq  ======================================")
    appFreq.show(10)

    val appCat0 = appCat
      .join(appFreq, Seq("appName"), "left")
      .select("cat", "appName", "appName0", "count").filter("appName0 is not NULL").distinct()
      .toDF()

    appCat0.write.mode("overwrite").saveAsTable("test.appCat0_sjq")

    println("appCat0 has " + appCat0.count() + " lines. ")
    println("==============================  appCat0  ==========================================")
    appCat0.show(10)

    val uidApp = getUidApp(spark, date, days - 1, appCat0).cache() //"uid", "cat", "date"

    println("uidApp has " + uidApp.count() + " lines. ")
    println("=============================== uidApp =============================================")
    uidApp.show(10)

    //    create table if not exists dl_cpc.hottopic_uid_bag
    //    (
    //      uid STRING COMMENT '用户id',
    //      cat STRING COMMENT 'app类别'
    //    )
    //    comment '段子社交、短视频、直播人群包'
    //    partitioned by (`date` string);

    uidApp.write.mode("overwrite").insertInto("dl_cpc.hottopic_uid_bag")

    upDate(spark, date)


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

  def getUidApp(spark: SparkSession, date: String, days: Int, appCat: DataFrame) = {
    /** appCat字段： cat, appName, appName0 */
    import spark.implicits._

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance
    val yesterday = sdf.parse(date)
    calendar.setTime(yesterday)
    calendar.add(Calendar.DATE, -days)
    val firstDay = calendar.getTime
    val date0 = sdf.format(firstDay)

    val sql =
      s"""
         | select
         |  t1.uid,
         |  t2.pkgs1
         |from (
         | select
         |  dt,
         |  uid
         | from dl_cpc.slim_union_log
         |where dt between '$date0' and '$date'
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  and antispam = 0
         |  and media_appsid = '80002819'
         | group by dt, uid ) t1
         | join (
         |   select
         |    load_date,
         |    uid,
         |    concat_ws(',', app_name) as pkgs1
         |   from dl_cpc.cpc_user_installed_apps
         |  where load_date between '$date0' and '$date'
         |  group by load_date, uid, app_name
         | ) t2
         | on t1.uid = t2.uid and t1.dt = t2.load_date
         | group by t1.uid, t2.pkgs1
       """.stripMargin
    println(sql)
    val df = spark.sql(sql)

    val result = df.rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[String]("pkgs1").split(",")))
      .flatMap(x => {
        val uid = x._1
        val pkgs = x._2
        val lb = scala.collection.mutable.ListBuffer[UidApp]()
        for (app <- pkgs) {
          lb += UidApp(uid, app)
        }
        lb
      }).toDF() // uid, appName0
      .join(appCat, Seq("appName0"))
      .select("uid", "cat")
      .distinct().toDF()
      .withColumn("date", lit(date))

    result
  }

  def upDate(spark: SparkSession, date: String): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance
    val today = sdf.parse(date)
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime()
    val date0 = sdf.format(yesterday)

    val sqlRequest =
      s"""
         |select
         |  coalesce(t1.cat, t2.cat) as cat,
         |  coalesce(t1.uid, t2.uid) as uid,
         |  tag0,
         |  tag1
         |from
         |  (
         |    select
         |      cat,
         |      uid,
         |      1 as tag1
         |    from
         |      dl_cpc.hottopic_uid_bag
         |    where
         |      `date` = '$date'
         |  ) t1 full
         |  outer join (
         |    select
         |      cat,
         |      uid,
         |      1 as tag0
         |    from
         |      dl_cpc.hottopic_uid_bag
         |    where
         |      `date` = '$date0'
         |  ) t2 on t1.cat = t2.cat
         |  and t1.uid = t2.uid
       """.stripMargin

    println(sqlRequest)
    val df = spark.sql(sqlRequest)
      .withColumn("id", when(col("cat") === "社交", lit(317)).otherwise(when(col("cat") === "短视频", lit(318)).otherwise(lit(319))))
      .withColumn("io", when(col("tag1").isNotNull, lit(true)).otherwise(lit(false)))
      .select("cat", "uid", "tag0", "tag1", "id", "io")

    df.write.mode("overwrite").saveAsTable("test.putOrDrop_sjq")

    val rdd1 = df.select("uid", "id", "io").rdd.map(x => (x.getAs[String]("uid"), x.getAs[Int]("id"), x.getAs[Boolean]("io")))

    val result = SetUserProfileTagInHiveDaily(rdd1)

  }

  case class AppCat(var appName: String, var cat: String)

  case class AppCount(var appName0: String, var appName1: String, appName: String, var count: Int)

  case class UidApp(var uid: String, var appName0: String)

}








