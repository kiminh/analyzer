package com.cpc.spark.common

import javax.mail.internet.InternetAddress

import com.github.jurajburian.mailer._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by roydong on 09/08/2017.
  */
object Utils {

  def deleteHdfs(path: String): Boolean = {
    val conf = new Configuration()
    val p = new Path(path)
    val hdfs = FileSystem.get(conf)
    hdfs.delete(p, true)
  }

  def sendMail(txt: String, sub: String, to: Seq[String]): Boolean = {
    val content: Content = new Content().text(txt)
    val conf = ConfigFactory.load()
    val session = (SmtpAddress(conf.getString("mail.host"), conf.getInt("mail.port")) :: SessionFactory())
      .session(Some(conf.getString("mail.username") -> conf.getString("mail.password")))
    val toAdd = to.map(new InternetAddress(_))
    val msg = Message(
      from = new InternetAddress(conf.getString("mail.sender")),
      subject = sub,
      content = content,
      to = toAdd)
    val mailer = Mailer(session)
    try {
      mailer.send(msg)
      true
    } catch {
      case e: Exception =>
        println(e.getMessage)
        false
    }
  }
}



