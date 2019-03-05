package com.cpc.spark.qukan.interest

import com.typesafe.config.ConfigFactory
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
  * Created by roydong on 06/12/2017.
  */
object GetAppCateFromBaidu {

  def main(args: Array[String]): Unit = {
    val url = "https://appc.baidu.com/as?pn=%d&st=10a001&subsearch=app&tn=appsite&word=%s"
    val word = args(0)
    var pkgs = Seq[(String, String)]()
    for (page <- 1 to 100) {
      val txt = Source.fromURL(url.format(page, word), "UTF8").mkString
      val names: Seq[(String, String)] = for {
        JObject(obj) <- parse(txt)
        JField("result_data", JArray(arr)) <- obj
        JObject(v) <- arr
        JField("package", JString(pkg)) <- v
        JField("sname", JString(name)) <- v
      } yield (name, pkg)

      pkgs = pkgs ++ names
    }

    pkgs.distinct.foreach {
      x =>
        println("{name: \"%s\", pkg: \"%s\"}".format(x._1, x._2))
    }
  }
}

