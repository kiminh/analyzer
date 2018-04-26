package com.cpc.spark.installapp.apptag

import scala.io.Source

/**
  * Created by roydong on 31/05/2017.
  */
object AnalTags {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: <csv_file>
           |
        """.stripMargin)
      System.exit(1)
    }
    var tags = Seq[String]()
    for (line <- Source.fromFile(args(0), "UTF8").getLines()) {
      val col = line.split(" ")
      if (col.length > 2) {
        col.slice(2, col.length).filter(!tags.contains(_)).foreach(x => tags :+= x)
      }
    }
    println(tags.mkString(" "))
  }
}
