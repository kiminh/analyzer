package com.cpc.spark.ml.cvrmodel.v4

import java.sql.DriverManager
import java.util.Properties

import scala.collection.JavaConversions._
import com.hankcs.hanlp.HanLP
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by roydong on 18/09/2017.
  */
object GetAdMaterial {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()
    val mariadbProp = new Properties()
    mariadbProp.put("url", conf.getString("mariadb.adv.url"))
    mariadbProp.put("user", conf.getString("mariadb.adv.user"))
    mariadbProp.put("password", conf.getString("mariadb.adv.password"))
    mariadbProp.put("driver", conf.getString("mariadb.adv.driver"))

    Class.forName(mariadbProp.getProperty("driver"))
    val conn = DriverManager.getConnection(
      mariadbProp.getProperty("url"),
      mariadbProp.getProperty("user"),
      mariadbProp.getProperty("password"))
    val stmt = conn.createStatement()
    val result = stmt.executeQuery("select id, title from idea")
    var rows = Seq[(Int, String)]()
    var allwords = Seq[String]()
    while (result.next()) {
      val id = result.getInt("id")
      val title = result.getString("title")
      rows = rows :+ (id, title)
      allwords = allwords :+ title
    }

    val numreg = """[0-9]+""".r
    rows.take(100)
      .foreach {
        x =>
          val terms = HanLP.segment(x._2)
            .filter(_.length() > 1)
            .filter(x => numreg.findFirstMatchIn(x.word).isEmpty)
            .map {
              x =>
                x.word
            }

          println(terms.mkString(" "))
      }

    println("-----")
    val dict = mutable.Map[String, Int]()
    val terms = HanLP.segment(allwords.mkString(" "))
      .filter(_.length() > 1)
      .filter(x => numreg.findAllMatchIn(x.word).isEmpty)
      .map {
        x =>
          dict.update(x.word, 1)
          x.word
      }

    println(dict.size)
  }
}
