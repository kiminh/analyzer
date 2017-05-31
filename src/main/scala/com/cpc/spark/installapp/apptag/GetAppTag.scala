package com.cpc.spark.installapp.apptag

import com.typesafe.config.ConfigFactory

import scala.io.Source

/**
  * Created by roydong on 31/05/2017.
  */
object GetAppTag {

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

    var n = 0
    for (line <- Source.fromFile(args(0), "UTF8").getLines()) {
      val nodes = line.split(",")
      println(line, nodes.length)
    }
  }
}



