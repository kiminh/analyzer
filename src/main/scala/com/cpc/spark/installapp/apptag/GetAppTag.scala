package com.cpc.spark.installapp.apptag

import java.io.{File, PrintWriter}

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

    val titleReg = """<span\sclass="title"\sitemprop="name">([^<]+)</span>""".r
    val reg1 = """<dt>分类</dt>([\s\S]*)<dt>TAG</dt>""".r
    val reg2 = """<dt>TAG</dt>([\s\S]*)<dt>更新</dt>""".r
    val nameReg1 = """appTag\">([^<]+)</a>""".r
    val nameReg2 = """\w+">([^<]+)</a>""".r
    val out = new PrintWriter(new File("../app_tag.txt"))
    for (line <- Source.fromFile(args(0), "UTF8").getLines()) {
      val nodes = line.split(",")
      if (nodes.length == 1) {
        var row = Seq[String]()
        val appName = nodes(0)
        row :+= appName
        val url = "http://www.wandoujia.com/apps/%s".format(appName)
        val html = Source.fromURL(url, "UTF8").mkString
        titleReg.findFirstMatchIn(html).foreach {
          m =>
            if (m.subgroups.length == 1) {
              row :+= m.subgroups(0)
            }
        }
        reg1.findFirstMatchIn(html).foreach {
          m =>
            if (m.subgroups.length == 1) {
              val part = m.subgroups(0).replaceAll("""[\s\r\n\t]""", "")
              nameReg1.findAllMatchIn(part).foreach {
                m1 =>
                  if (m1.subgroups.length == 1) {
                    row :+= m1.subgroups(0)
                  }
              }
            }
        }
        reg2.findFirstMatchIn(html).foreach {
          m =>
            if (m.subgroups.length == 1) {
              val part = m.subgroups(0).replaceAll("""[\s\r\n\t]""", "")
              nameReg2.findAllMatchIn(part).foreach {
                m1 =>
                  if (m1.subgroups.length == 1) {
                    //println(m1.subgroups(0))
                  }
              }
            }
        }
        out.write(row.mkString(" ") + "\n")
        println(row.mkString(" "))
      }
    }
    out.close()
  }
}



