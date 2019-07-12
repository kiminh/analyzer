package com.cpc.spark.conversionMonitor

import com.github.jurajburian.mailer._
import javax.mail.internet.InternetAddress
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object monitorApiCvr {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

//    val cnt3 = getDataV3(date, hour, spark)
//    val cnt4 = getDataV4(date, hour, spark)
//    val cnt5 = getDataV5(date, hour, spark)
//    println(s"v3 = $cnt3, v4 = $cnt4, v5 = $cnt5")
//
//    var message = ""
//
//    val cntDiffPercent = (cnt3.toDouble - cnt4.toDouble) / cnt3.toDouble
//    if (cntDiffPercent < 0 || cntDiffPercent > 0.15) {
//      message = message + "v4 abnormal\n"
//    }
//    if (cnt3 != cnt5) {
//      message = message + "v5 abnormal\n"
//    }
//    val sub = "api conversion monitor warning!"
//    var receiver = Seq[String]()
//    receiver:+="wangjun02@qutoutiao.net"
////    receiver:+="hanzhengding@qutoutiao.net"
////    receiver:+="zhanghongyang@qutoutiao.net"
////    receiver:+="wangyao@qutoutiao.net"
////    receiver:+="dongjinbao@qutoutiao.net"
////    receiver:+="chuquanquan@qutoutiao.net"
//    println(message)
//    if (message != "") {
//      message += s"date=$date, hour=$hour: v3 = $cnt3, v4 = $cnt4, v5 = $cnt5"
//      sendMail(message, sub, receiver)
//    }


  }



  def sendMail(txt: String, sub: String, to: Seq[String]): Boolean = {
    //val conf = ConfigFactory.load()
    val session = (SmtpAddress("smtp.exmail.qq.com", 25) :: SessionFactory())
      .session(Some("cpc_notify@aiclk.com" -> "nXBYjc8XVB6hCPv5"))
    val toAdd = to.map(new InternetAddress(_))
    val msg = Message(
      from = new InternetAddress("cpc_notify@aiclk.com"),
      subject = sub,
      content = Content().text(txt),
      to = toAdd)
    try {
      Mailer(session).send(msg)
      true
    } catch {
      case e: Exception =>
        println(e.getMessage)
        false
    }
  }


}



