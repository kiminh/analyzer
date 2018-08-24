package com.cpc.spark.log.anal

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.anal.TopCtrIdea.Adinfo
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


/**
  * 在TopCtrIdeaV2基础上做修改
  * 1. 添加推荐素材新类型：(已有：1为小图，2为长图，3组图)4为视频，6为文本 7互动 8开屏 9 横幅
  * 2. 删除就系统的数据：adv_old
  * 3. 删除ctr等比缩放
  * 4. 根据adslot_type等比取80000数据
  */
object TopCtrIdeaV2 {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before> <int> <table:string>
           |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt //10
    val table = args(1) //top_ctr_idea

    val spark = SparkSession.builder()
      .appName("top ctr ideas")
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)

    //读取近10天广告信息。 ((adslot_type, ideaid), Adinfo)
    //var adctr: RDD[((Int, Int), Adinfo)] = null


    val dateStart = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, dayBefore - 1)
    val dateEnd = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val stmt =
      """
        |select adslot_type, ideaid, sum(isclick) as sum_click, sum(isshow) as sum_show
        |from dl_cpc.cpc_union_log where `date` >= "%s" and `date` <= "%s"
        |and isshow = 1 and adslotid > 0 and ideaid > 0
        |group by adslot_type, ideaid
      """.stripMargin.format(dateStart, dateEnd)
    println(stmt)

    val ulog = spark.sql(stmt)
      .rdd
      .map {
        u =>
          val key = (u.getAs[Int]("adslot_type"), u.getAs[Int]("ideaid"))
          val v = Adinfo(
            idea_id = u.getAs[Int]("ideaid"),
            adslot_type = u.getAs[Int]("adslot_type"),
            click = u.getAs[Long]("sum_click").toInt,
            show = u.getAs[Long]("sum_show").toInt)

          (key, v)
      }



    /**
      * 计算ctr. ctr=click/show*1e6
      * 返回 Adinfo
      */
    val adinfo2 = ulog
      .map {
        x =>
          val v = x._2 //Adinfo
        val ctr = (v.click.toDouble / v.show.toDouble * 1e6).toInt
          v.copy(ctr = ctr)
      }
      .filter(x => x.click > 0 && x.show > 1000)

    println("总条数： " + adinfo2.count())

    val adinfo = adinfo2.toLocalIterator
      .toSeq

    val ub = getUserBelong() //获取广告主id, 代理账户id  Map[id, belong]
    val titles = getIdeaTitle() //从adv.idea表读取数据  Map[id, (title, image,type,video_id,user_id,category)]
    val imgs = getIdaeImg() //从adv.resource表读取素材资源  Map[id, (remote_url, type)]


    println("title length: " + titles.size)
    println("imgs length: " + imgs.size)

    var id = 0
    val topIdeaRDD = adinfo
      .map {
        x => //Adinfo
          val ad = titles.getOrElse(x.idea_id, null) //根据ideaid获得  (title,image,type,video_id,user_id,category)
          if (ad != null) {
            var mtype = ad._3 //type

            var img: Seq[(String, Int)] = Seq()
            if (mtype != 6) {
              img = ad._2.split(",").map(_.toInt).toSeq
                .map(x => imgs.getOrElse(x, null)).filter(_ != null) //获得image的type和remote_url (remote_url,type)
            }
            val video = imgs.getOrElse(ad._4, null) //获得video的type和remote_url (remote_url,type)


            val adclass = (ad._6 / 1000000) * 1000000 + 100100

            id += 1
            var topIdea = TopIdea(
              id = id,
              user_id = ad._5,
              idea_id = x.idea_id,
              agent_id = ub.getOrElse(ad._5, 0),
              adclass = ad._6,
              adclass_1 = adclass,
              adslot_type = x.adslot_type,
              title = ad._1, //title
              mtype = mtype, //type
              ctr_score = x.ctr,
              from = "cpc_adv",
              show = x.show,
              click = x.click
            )

            if (mtype == 4) { //视频
              topIdea = topIdea.copy(images = video._1)
            } else { //除视频以外其它
              topIdea = topIdea.copy(images = img.map(_._1).mkString(" "))
            }

            topIdea

          } else {
            null
          }
      }
      .filter(_ != null)


    val sum = topIdeaRDD.length.toDouble //总元素个数
    println("总元素个数：" + sum)

    val adslot_type: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7)
    var rate_map: mutable.Map[Int, Double] = mutable.HashMap()
    var max_ctr_map: mutable.Map[Int, Int] = mutable.HashMap()

    for (i <- adslot_type) {
      var tmp = topIdeaRDD.filter(_.adslot_type == i)


      println("adslot_type=" + i + "有 " + tmp.length + " 条")

      if (tmp.length > 0) {
        //计算占比
        var r = tmp.length / sum
        rate_map.put(i, r)

        //计算最大ctr
        var max_ctr = tmp.map(_.ctr_score).max
        max_ctr_map.put(i, max_ctr)
      }

    }

    println("占比：" + rate_map)
    for ((x, y) <- max_ctr_map) {
      println("adslot_type: " + x + "; max_ctr: " + y)
    }

    for (i <- adslot_type) {
      println("adslot_type=" + i + "取 " + (80000 * (rate_map.getOrElse[Double](i, 0.0))).toInt + " 条")
    }


    var topIdeaData = mutable.Seq[TopIdea]()

    for (i <- 0 until adslot_type.length) {
      val topIdeaRDD2 = topIdeaRDD.filter(x => x.adslot_type == adslot_type(i))
        .sortWith(_.ctr_score > _.ctr_score).take((80000 * (rate_map.getOrElse[Double](i, 0.0))).toInt)

      println("adslot_type=" + adslot_type(i) + "已取出 " + topIdeaRDD2.length + " 条")
      topIdeaData = topIdeaData ++ topIdeaRDD2
    }


    if (topIdeaData.length > 0) {
      println("###### res: " + topIdeaData(0))
    }


    val conf = ConfigFactory.load()
    val mariadbUrl = conf.getString("mariadb.url")
    val mariadbProp = new Properties()
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    //truncate table
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |TRUNCATE TABLE report.%s
        """.stripMargin.format(table)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("truncate table failed : " + e);
    }

    spark.createDataFrame(topIdeaData)
      .drop("adslot_type", "show", "click")
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report." + table, mariadbProp)


    println("###### num: " + topIdeaData.length)

  }

  def getUserBelong(): Map[Int, Int] = {
    val sql = "select id,belong from user" //广告主id, 代理账户id
    val rs = getAdDbResult("mariadb.adv", sql)
    val ub = mutable.Map[Int, Int]()
    while (rs.next()) {
      ub.update(rs.getInt("id"), rs.getInt("belong"))
    }

    ub.toMap
  }

  def getIdeaTitle(): Map[Int, (String, String, Int, Int, Int, Int)] = {
    //荐素材类型: 1为小图，2为长图，3组图，4为视频，6为文本 7互动 8开屏 9 横幅
    var sql = "select id, title, image, type, video_id, user_id, category from idea where action_type = 1 and type in (1,2,3,4,6,7,8,9)"
    val ideas = mutable.Map[Int, (String, String, Int, Int, Int, Int)]()
    var rs = getAdDbResult("mariadb.adv", sql)
    while (rs.next()) {
      val idea = (rs.getString("title"), rs.getString("image"),
        rs.getInt("type"), rs.getInt("video_id"),
        rs.getInt("user_id"), rs.getInt("category"))
      val id = rs.getInt("id")
      ideas.update(id, idea)
    }

    ideas.toMap
  }

  def getIdaeImg(): Map[Int, (String, Int)] = {
    val sql = "select id,`type`,remote_url from resource"
    val images = mutable.Map[Int, (String, Int)]()
    var rs = getAdDbResult("mariadb.adv", sql)
    while (rs.next()) {
      images.update(rs.getInt("id"), (rs.getString("remote_url"), rs.getInt("type")))
    }

    images.toMap
  }

  def getAdDbResult(confKey: String, sql: String): ResultSet = {
    val conf = ConfigFactory.load()
    val mariadbProp = new Properties()
    mariadbProp.put("url", conf.getString(confKey + ".url"))
    mariadbProp.put("user", conf.getString(confKey + ".user"))
    mariadbProp.put("password", conf.getString(confKey + ".password"))
    mariadbProp.put("driver", conf.getString(confKey + ".driver"))

    Class.forName(mariadbProp.getProperty("driver"))
    val conn = DriverManager.getConnection(
      mariadbProp.getProperty("url"),
      mariadbProp.getProperty("user"),
      mariadbProp.getProperty("password"))
    val stmt = conn.createStatement()
    stmt.executeQuery(sql)
  }

  private case class Adinfo(
                             agent_id: Int = 0,
                             user_id: Int = 0,
                             idea_id: Int = 0,
                             adclass: Int = 0,
                             adslot_type: Int = 0,
                             click: Int = 0,
                             show: Int = 0,
                             ctr: Int = 0,
                             ctr_type: Int = 0
                           ) {

  }

  private case class TopIdea(
                              id: Int = 0,
                              agent_id: Int = 0,
                              user_id: Int = 0,
                              idea_id: Int = 0,
                              adclass: Int = 0,
                              adclass_1: Int = 0,
                              adslot_type: Int = 0,
                              title: String = "",
                              mtype: Int = 0,
                              images: String = "",
                              ctr_score: Int = 0,
                              from: String = "",
                              click: Int = 0,
                              show: Int = 0
                            )

}



