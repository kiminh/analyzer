package com.cpc.spark.util


import org.json4s._
import org.json4s.native.JsonMethods.parse

import scala.io.Source

object GetAppCateFromBaiduUil {

  def getAppCate(name: String, page: Int) = {
    val url = "https://appc.baidu.com/as?pn=%d&st=10a001&subsearch=app&tn=appsite&word=%s"
    var pkgs = List[(String, String)]()
    for (page <- 1 to page) {
      try {
        val txt = Source.fromURL(url.format(page, name), "UTF8").mkString
        val names: Seq[(String, String)] = for {
          JObject(obj) <- parse(txt)
          JField("result_data", JArray(arr)) <- obj
          JObject(v) <- arr
          JField("package", JString(pkg)) <- v
          JField("sname", JString(name)) <- v
        } yield (name, pkg)

        pkgs = pkgs ++ names
      } catch {
        case e: Exception =>
      }
    }
    pkgs
  }
}
