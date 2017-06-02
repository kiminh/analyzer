package com.cpc.spark.installapp.apptag

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

/**
  * Created by roydong on 02/06/2017.
  */
object InitTagConf {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: <csv_file>
           |
        """.stripMargin)
      System.exit(1)
    }

    val conf = ConfigFactory.load()
    val tagsConf = conf.getStringList("app_tag.v2.tags")

    var n = 1
    val tags = mutable.Map[String, Int]()
    tagsConf.toIterator.foreach {
      name =>
        val id = 50000 + n * 100
        tags.update(name, id)
        n += 1
    }

    /*
    tags.foreach{
      case (name, id) =>
        println(name + "," + id)
    }
    */

    val appTags = mutable.Map[String, Seq[Int]]()
    for (line <- Source.fromFile(args(0), "UTF8").getLines()) {
      val col = line.split(" ")
      if (col.length > 1) {
        val appName = col(0)
        var ids = Seq[Int]()
        col.slice(1, col.length).foreach {
          name =>
            val id = tags.getOrElse(name, 0)
            if (id > 0) {
              ids = ids :+ id
            }
        }
        if (ids.length > 0) {
          appTags.update(appName, ids)
        }
      }
    }

    appTags.foreach {
      case (appName, ids) =>
        println("\"%s\": [%s],".format(appName.replace('.', '|'), ids.mkString(", ")))
    }
  }
}


