package com.cpc.spark.qukan.anal

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by Roy on 2017/5/5.
  */
object AnalAppCategory {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: <csv_file>
           |
        """.stripMargin)
      System.exit(1)
    }
    var n = 0
    for (line <- Source.fromFile(args(0), "UTF8").getLines()) {
      val nodes = line.split(",")
      if (n > 1) {
        if (nodes.length > 2) {
          val pc = PkgCate(name = nodes(0), desc = nodes(1))
          for (i <- 2 to nodes.length - 1) {
            val v = nodes(i)
            if (v.length > 0) {
              pc.tags = pc.tags :+ i * 1000
            }
          }
          if (pc.tags.length > 0) {
            println("\"%s\": [%s],".format(pc.name.replace('.', '|'), pc.tags.mkString(",")))
          }
        }
      }
      n = n + 1
    }
  }
}

case class PkgCate(
                  name: String = "",
                  desc: String = "",
                  var tags: Array[Int] = Array()
                  )
