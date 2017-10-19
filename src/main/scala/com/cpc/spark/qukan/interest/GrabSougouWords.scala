package com.cpc.spark.qukan.interest

import java.io.FileWriter

import scala.io.Source

/**
  * Created by roydong on 18/10/2017.
  */
object GrabSougouWords {

  def main(args: Array[String]): Unit = {
    //val idxUrl = "http://pinyin.sogou.com/dict/cate/index/436/default/%d"   //电子游戏
    //val idxUrl = "http://pinyin.sogou.com/dict/search/search_list/%C6%E5%C5%C6/normal/"  //棋牌
    val idxUrl = "http://pinyin.sogou.com/dict/cate/index/605/default/" //手机游戏
    val dlbtnReg = """<div\sclass="dict_dl_btn"><a\shref="([^<]+)"></a></div>""".r

    var n = 0
    val w = new FileWriter("./sougou-words-url.txt")
    for (page <- 1 to 9) {
      val html = Source.fromURL(idxUrl + page.toString, "UTF8").mkString

      dlbtnReg.findAllMatchIn(html).foreach {
        m =>
          m.subgroups.foreach {
            dlurl =>
              val dlurl = m.subgroups.head
              println(page, dlurl)
              w.write(dlurl + "\n")
          }
      }
    }
    w.close()
  }
}
