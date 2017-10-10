package com.cpc.spark.ml.common

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import com.hankcs.hanlp.HanLP
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by roydong on 20/09/2017.
  */
object DspData {


  def getAdDbResult(confKey: String): ResultSet = {
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
    stmt.executeQuery("select id, user_id, plan_id, target_url, title from idea where action_type = 1")
  }

  def getAdDict(): Map[Int, AdInfo] = {
    val numreg = """[0-9]+""".r
    var rows = Seq[(Int, Int, Int, String, Seq[String])]()
    var adids = Seq[Int]()
    var userids = Seq[Int]()
    var planids = Seq[Int]()
    var urls = Seq[String]()
    var titles = Seq[String]()
    var result = getAdDbResult("mariadb.adv")

    var maxTitleLen = 0
    def mergeData(): Unit = {
      val titleWords = HanLP.segment(result.getString("title"))
        .filter(x => x.length() > 1 && numreg.findAllMatchIn(x.word).isEmpty)
        .map(_.word)

      if (maxTitleLen < titleWords.length) {
        maxTitleLen = titleWords.length
      }

      val row = (result.getInt("id"), result.getInt("user_id"), result.getInt("plan_id"),
        result.getString("target_url"), titleWords)
      rows = rows :+ row
      adids :+= row._1
      userids :+= row._2
      planids :+= row._3
      urls :+= row._4
      titles ++= titleWords
    }
    while (result.next()) {
      mergeData()
    }
    result = getAdDbResult("mariadb.adv_old")
    while (result.next()) {
      mergeData()
    }
    println("max title len", maxTitleLen)

    var n = 0
    val adidmap = mutable.Map[Int, Int]()
    adids.distinct.foreach{
      x =>
        n += 1
        adidmap.update(x, n)
    }

    println("adid", adidmap.size, n)

    n = 0
    val useridmap = mutable.Map[Int, Int]()
    userids.distinct.foreach{
      x =>
        n += 1
        useridmap.update(x, n)
    }
    println("userid", useridmap.size, n)

    n = 0
    val planidmap = mutable.Map[Int, Int]()
    planids.distinct.foreach{
      x =>
        n += 1
        planidmap.update(x, n)
    }
    println("planid", planidmap.size, n)

    n = 0
    val urlmap = mutable.Map[String, Int]()
    urls.distinct.foreach{
      x =>
        n += 1
        urlmap.update(x, n)
    }
    println("url", urlmap.size, n)

    n = 0
    val titlemap = mutable.Map[String, Int]()
    titles.distinct
      .foreach {
        w =>
          n += 1
          titlemap.update(w, n)
      }
    println("title", titlemap.size, n)

    val admap = mutable.Map[Int, AdInfo]()
    rows
      .foreach {
        x =>
          val titleidx = x._5.map(titlemap.getOrElse(_, 0))
          val ad = AdInfo(
            adid = adidmap(x._1),
            userid = useridmap(x._2),
            planid = planidmap(x._3),
            turl = urlmap(x._4),
            titles = titleidx
          )
          admap.update(x._1, ad)
      }

    admap.take(5).foreach(println)
    admap.toMap
  }

  case class AdInfo(adid: Int, userid: Int, planid: Int, turl: Int, titles: Seq[Int]) {

  }
}
