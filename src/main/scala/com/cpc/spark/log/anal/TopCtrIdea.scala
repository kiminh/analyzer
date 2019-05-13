package com.cpc.spark.log.anal

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by roydong on 27/10/2017.
  */
object TopCtrIdea {


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
    val dayBefore = args(0).toInt
    val table = args(1)

    val spark = SparkSession.builder()
      .appName("top ctr ideas")
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    var adctr: RDD[((Int, Int), Adinfo)] = null
    for (i <- 0 until dayBefore) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val stmt = """
                   |select *
                   | from dl_cpc.cpc_basedata_union_events where `day` = "%s" and isshow = 1
                   |and adslot_id > 0 and adslot_type in (1,2)
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
        .reduceByKey {
          (x, y) =>
            x.copy(
              click = x.click + y.click,
              show = x.show + y.show
            )
        }
      if (adctr == null) {
        adctr = ulog
      }
      adctr = adctr.union(ulog)
      cal.add(Calendar.DATE, 1)
    }

    val adinfo = adctr
      .reduceByKey {
        (x, y) =>
          x.copy(
            click = x.click + y.click,
            show = x.show + y.show
          )
      }
      .map {
        x =>
          val v = x._2
          val ctr = (v.click.toDouble / v.show.toDouble * 1e6).toInt
          v.copy(ctr = ctr)
      }
      .filter(x => x.click > 0 && x.show > 1000)

    val max1 = adinfo.filter(_.adslot_type == 1).map(_.ctr).max()
    val max2 = adinfo.filter(_.adslot_type == 2).map(_.ctr).max()
    val rate = max1.toDouble / max2.toDouble

    val ub = getUserBelong()
    val titles = getIdeaTitle()
    val imgs = getIdaeImg()

    println(max1, max2, rate)
    var id = 0
    val top = adinfo.map(x => (x.idea_id, x))
      .reduceByKey {
        (x, y) =>
          if (x.adslot_type == 1) {
            x.copy(
              ctr = (x.ctr + (y.ctr.toDouble * rate).toInt) / 2,
              ctr_type = 1
            )
          } else {
            y.copy(
              ctr = (y.ctr + (x.ctr.toDouble * rate).toInt) / 2,
              ctr_type = 1
            )
          }
      }
      .map {
        x =>
          val ad = x._2
          if (ad.adslot_type == 2) {
            ad.copy(
              ctr = (ad.ctr.toDouble * rate).toInt
            )
          } else {
            ad
          }
      }
      .sortBy(x => x.ctr, false)
      .take(40000)
      .map {
        x =>
          val ad = titles.getOrElse(x.idea_id, null)
          if (ad != null) {
            val img = ad._2.map(x => imgs.getOrElse(x, null)).filter(_ != null)

            var mtype = 0
            if (img.length == 3) {
              mtype = 3
            } else if(img.length == 1) {
              val i = img(0)
              if (i._2 == 5) {
                mtype = 2  //大图
              } else {
                mtype = 1   //小图
              }
            }

            val adclass = (x.adclass / 1000000) * 1000000 + 100100

            id += 1
            TopIdea (
              id = id,
              user_id = x.user_id,
              idea_id = x.idea_id,
              agent_id = ub.getOrElse(x.user_id, 0),
              adclass = x.adclass,
              adclass_1 = adclass,
              title = ad._1,
              mtype = mtype,
              images = img.map(_._1).mkString(" "),
              ctr_score = x.ctr,
              from = "cpc_adv"
            )
          } else {
            null
          }
      }
      .filter(_ != null)

    val conf = ConfigFactory.load()
    val mariadbUrl = conf.getString("mariadb.url")
    val mariadbProp = new Properties()
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password",conf.getString("mariadb.password"))
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

    spark.createDataFrame(top)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report." + table, mariadbProp)

    println("num", top.length)
  }

  def getUserBelong(): Map[Int, Int] = {
    val sql = "select id,belong from user"
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

  def getIdeaTitle(): Map[Int, (String, Seq[Int])] = {
    var sql = "select id, title, image from idea where action_type = 1"
    val ideas = mutable.Map[Int, (String, Seq[Int])]()
    var rs = getAdDbResult("mariadb.adv", sql)
    while (rs.next()) {
      val idea = (rs.getString("title"), rs.getString("image").split(",").map(_.toInt).toSeq)
      val id = rs.getInt("id")
      ideas.update(id, idea)
    }
    rs = getAdDbResult("mariadb.adv_old", sql)
    while (rs.next()) {
      val idea = (rs.getString("title"), rs.getString("image").split(",").map(_.toInt).toSeq)
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
    rs = getAdDbResult("mariadb.adv_old", sql)
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
                              adclass: Int = 0,git 
                              adclass_1: Int = 0,
                              title: String = "",
                              mtype: Int = 0,
                              images: String = "",
                              ctr_score: Int = 0,
                              from: String = ""
                            )
}


