package com.cpc.spark.common

import java.io.{File, FileOutputStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.ml.calibration.HourlyCalibration.localDir
import javax.mail.internet.InternetAddress
import com.github.jurajburian.mailer._
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import spire.math.ULong

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
    val conf = ConfigFactory.load()
    val session = (SmtpAddress(conf.getString("mail.host"), conf.getInt("mail.port")) :: SessionFactory())
      .session(Some(conf.getString("mail.username") -> conf.getString("mail.password")))
    val toAdd = to.map(new InternetAddress(_))
    val msg = Message(
      from = new InternetAddress(conf.getString("mail.sender")),
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

  //得到所有排列组合 C(n, m)
  def getCombination[T: Manifest](all: Seq[T], n: Int): Seq[Seq[T]] = {
    var combs = Seq[Seq[T]]()
    val comb = new Array[T](n)
    def mapCombination(n: Int, start: Int, idx: Int, comb: Array[T]): Unit = {
      if (n > 0) {
        for (i <- start until all.length) {
          comb(idx) = all(i)
          mapCombination(n - 1, i + 1, idx + 1, comb)
        }
      } else {
        var seq = Seq[T]()
        comb.foreach {
          v => seq = seq :+ v
        }
        combs :+= seq
      }
    }
    mapCombination(n, 0, 0, comb)
    combs
  }


  def buildSparkSession(name : String, serializer: String = "org.apache.spark.serializer.KryoSerializer",
                        buffer : String = "2047MB", enableHiveSupport : Boolean = true): SparkSession = {
    val builder = SparkSession.builder()
      .config("spark.serializer", serializer)
      .config("spark.kryoserializer.buffer.max", buffer)
      .appName(name)
    if (enableHiveSupport) {
      builder.enableHiveSupport()
    }
    return builder.getOrCreate()
  }

  def getCtrModelIdFromExpTags(expTags: String): String = {
    val prefix = "ctrmodel="
    expTags.split(",").foreach(
      x =>
        if (x.startsWith(prefix)) {
          var model = x.substring(prefix.length)
          if (model.startsWith("0-") || model.startsWith("1-")) {
            model = model.substring(2)
          }
          if (model.endsWith("-uid")) {
            model = model.substring(0, model.length - 4)
          }
          return model
        }
    )
    return "undefined"
  }

  def getTimeRangeSql(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour >= '$startHour')"
    }
    return s"((`date` = '$startDate' and hour >= '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }

  def djb2Hash(str: String): ULong = {
    var hash = new ULong(5381)
    str.foreach(c => {
      hash = ((hash << 5) + hash) + new ULong(c) /* hash * 33 + c */
    })
    return hash
  }

  def saveProtoToFile[T <: com.trueaccord.scalapb.GeneratedMessage](obj: T, localPath: String): Unit = {
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    obj.writeTo(new FileOutputStream(localPath))
  }

  def getStartDateHour(endDate: String, endHour: String, hourRange: Int): (String, String) = {
    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    return (startDate, startHour)
  }
}



