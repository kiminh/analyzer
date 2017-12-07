package com.cpc.spark.qukan.interest

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

/**
  * Created by roydong on 05/12/2017.
  */
object CheckKMeansUserApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("tag user by installed apps")
      .getOrCreate()

    var cluster = 0
    val userPkgs = mutable.Map[String, Seq[String]]()
    var n = 0
    var samePkgs = Seq[String]()
    for (line <- Source.fromFile("kmeans1.txtx").getLines()) {
      if (line.length > 0) {
        val seps = line.split(" ")
        if (seps.length > 1 ) {
          if (seps(0) == "cluster") {
            cluster = seps(1).toInt
            if (cluster > 0) {
              println("cluster", cluster - 1)
              n = 0
              println(samePkgs)
            }
          } else {
            val uid = seps(0)
            val pkgs = seps.slice(1, seps.length)
            if (n == 0) {
              samePkgs = pkgs
            } else {
              samePkgs = samePkgs.intersect(pkgs)
            }
          }
        }
      }
    }


  }
}
