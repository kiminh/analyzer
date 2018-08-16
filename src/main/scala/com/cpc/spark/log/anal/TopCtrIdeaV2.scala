package com.cpc.spark.log.anal

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable


/**
  * 在TopCtrIdeaV2基础上做修改
  * 1. 添加推荐素材新类型：4-视频  6-文本  7-互动  9-开屏  9-横幅
  * 2. 删除就系统的数据：adv_old
  * 3. 删除ctr等比缩放
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
    var adctr: RDD[((Int, Int), Adinfo)] = null

    for (i <- 0 until dayBefore) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val stmt =
        """
          |select *,
          | ext['adclass'].int_value as adclass
          | from dl_cpc.cpc_union_log where `date` = "%s" and isshow = 1
          |and adslotid > 0 and adslot_type in (1,2)
        """.stripMargin.format(date)
      println(stmt)
      val ulog = spark.sql(stmt)
        //        .as[UnionLog]
        .rdd
        .map {
          u =>
            val key = (u.getAs[Int]("adslot_type"), u.getAs[Int]("ideaid"))
            val cls = u.getAs[Int]("adclass")
            val v = Adinfo(
              user_id = u.getAs[Int]("userid"),
              idea_id = u.getAs[Int]("ideaid"),
              adslot_type = u.getAs[Int]("adslot_type"),
              adclass = cls,
              click = u.getAs[Int]("isclick"),
              show = u.getAs[Int]("isshow"))

            (key, v)
        }
        .reduceByKey { //计算当天的click,show
          (x, y) =>
            x.copy(
              click = x.click + y.click,
              show = x.show + y.show
            )
        }
      if (adctr == null) {
        adctr = ulog
      }else{
        adctr = adctr.union(ulog) //合并
      }

      cal.add(Calendar.DATE, 1)
    }

    /**
      * 计算ctr. ctr=click/show*1e6
      * 返回 Adinfo
      */
    val adinfo = adctr
      .reduceByKey { //计算近10天的click,show
        (x, y) =>
          x.copy(
            click = x.click + y.click,
            show = x.show + y.show
          )
      }
      .map {
        x =>
          val v = x._2 //Adinfo
        val ctr = (v.click.toDouble / v.show.toDouble * 1e6).toInt
          v.copy(ctr = ctr)
      }
      .filter(x => x.click > 0 && x.show > 1000)

//    val max1 = adinfo.filter(_.adslot_type == 1).map(_.ctr).max() //列表页最大点击率
//    val max2 = adinfo.filter(_.adslot_type == 2).map(_.ctr).max() //详情页最大点击率
//    val rate = max1.toDouble / max2.toDouble

    val ub = getUserBelong() //获取广告主id, 代理账户id  Map[id, belong]
    val titles = getIdeaTitle() //从adv.idea表读取推广创意id,title,image  Map[id, (title, Seq[image],type,video_id)]
    val imgs = getIdaeImg() //从adv.resource表读取素材资源id, 远程下载地址,素材类型  Map[id, (remote_url, type)]

    //println(max1, max2, rate)
    adinfo.take(3).foreach(x => println(x))

    var id = 0
    val topIdeaRDD = adinfo
      .map {
        x => //Adinfo
          val ad = titles.getOrElse(x.idea_id, null) //根据ideaid获得title和image  (title,Seq(image),type,video_id)
          if (ad != null) {
            var mtype = 0
            mtype = ad._3 match {
              case 1 => 1 //小图
              case 2 => 2 //大图
              case 3 => 3 //组图
              case 4 => 4 //视频
              case 6 => 6 //文本, 没有图片和视频, image=0
              case 7 => 7 //互动
              case 8 => 8 //开屏
              case 9 => 9 //横幅
              case _ => -1
            }

            val img = ad._2.map(x => imgs.getOrElse(x, null)).filter(_ != null) //获得image的type和remote_url (remote_url,type)
            val video = imgs.getOrElse(ad._4, null) //获得video的type和remote_url (remote_url,type)

            val adclass = (x.adclass / 1000000) * 1000000 + 100100

            id += 1
            var topIdea = TopIdea(
              id = id,
              user_id = x.user_id,
              idea_id = x.idea_id,
              agent_id = ub.getOrElse(x.user_id, 0),
              adclass = x.adclass,
              adclass_1 = adclass,
              title = ad._1, //title
              mtype = mtype, //type
              ctr_score = x.ctr,
              from = "cpc_adv"
            )

            if (mtype == 4) { //视频
              topIdea = topIdea.copy(images = video.toString())
            } else { //除视频以外其它
              topIdea = topIdea.copy(images = img.map(_._1).mkString(" "))
            }

            topIdea

          } else {
            null
          }
      }
      .filter(_ != null)

    val sum = topIdeaRDD.count().toInt //总元素个数
    println("总元素个数：" + sum)

    val mtypeMap = topIdeaRDD.map(x => (x.mtype, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, x._2 / sum)).collectAsMap() //占比
    println("占比：" + mtypeMap)

    val max1 = topIdeaRDD.filter(_.mtype == 1).map(_.ctr_score).max()
    val max2 = topIdeaRDD.filter(_.mtype == 2).map(_.ctr_score).max()
    val max3 = topIdeaRDD.filter(_.mtype == 3).map(_.ctr_score).max()
    val max4 = topIdeaRDD.filter(_.mtype == 4).map(_.ctr_score).max()

    val max6 = topIdeaRDD.filter(_.mtype == 6).map(_.ctr_score).max()
    val max7 = topIdeaRDD.filter(_.mtype == 7).map(_.ctr_score).max()
    val max8 = topIdeaRDD.filter(_.mtype == 8).map(_.ctr_score).max()
    val max9 = topIdeaRDD.filter(_.mtype == 9).map(_.ctr_score).max()

    println("type=1最大ctr_score: "+max1
    +"type=2最大ctr_score: "+max2
    +"type=3最大ctr_score: "+max3
    +"type=4最大ctr_score: "+max4
    +"type=6最大ctr_score: "+max6
    +"type=7最大ctr_score: "+max7
    +"type=8最大ctr_score: "+max8
    +"type=9最大ctr_score: "+max9)


    //    import spark.implicits._
    //    val topIdeaRDD2 = topIdeaRDD.toDF("id", "agent_id", "user_id", "idea_id", "adclass", "adclass_1", "title", "mtype", "images", "ctr_score", "from")
    //      .repartition($"mtype").foreachPartition(
    //      p => {
    //        val pRdd = spark.sparkContext.parallelize(p.toSeq)
    //        val sortedParRdd = pRdd.sortBy(r => r.getAs[Int]("cty_score"), false)
    //          .take(40000 * (mtypeMap.getOrElse(1, 0)))
    //      }
    //    )

    val type_num: Array[Int] = Array(1, 2, 3, 4, 6, 7, 8, 9)
    var topIdeaData: Array[TopIdea] = Array()

    for (i <- 0 until type_num.length) {
      val topIdeaRDD2 = topIdeaRDD.filter(x => x.mtype == type_num(i)).sortBy(x => x.ctr_score, false)
        .take(40000 * (mtypeMap.getOrElse(type_num(i), 0)))

      if (topIdeaData == null) {
        topIdeaData = topIdeaRDD2
      }else{
        topIdeaData = topIdeaData ++ topIdeaRDD2
      }

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
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report." + table, mariadbProp)

    println("num", topIdeaData.length)
  }

  def getUserBelong(): Map[Int, Int] = {
    val sql = "select id,belong from user" //广告主id, 代理账户id
    val rs = getAdDbResult("mariadb.adv", sql)
    val ub = mutable.Map[Int, Int]()
    while (rs.next()) {
      ub.update(rs.getInt("id"), rs.getInt("belong"))
    }
    val rsold = getAdDbResult("mariadb.adv_old", sql)
    while (rsold.next()) {
      ub.update(rsold.getInt("id"), rsold.getInt("belong"))
    }
    ub.toMap
  }

  def getIdeaTitle(): Map[Int, (String, Seq[Int], Int, Int)] = {
    var sql = "select id, title, type, image, video_id from idea where action_type = 1 and type in (1,2,3,4,6,7,8,9)"
    val ideas = mutable.Map[Int, (String, Seq[Int], Int, Int)]()
    var rs = getAdDbResult("mariadb.adv", sql)
    while (rs.next()) {
      val idea = (rs.getString("title"), rs.getString("image").split(",").map(_.toInt).toSeq,
        rs.getInt("type"), rs.getInt("video_id"))
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
                              title: String = "",
                              mtype: Int = 0,
                              images: String = "",
                              ctr_score: Int = 0,
                              from: String = ""
                            )

}



