package com.cpc.spark.qukan.interest

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.util.parsing.json.JSON

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.train.LRIRModel
import com.cpc.spark.qukan.parser.HdfsParser
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.cpc.spark.streaming.tools.{Encoding, Gzip}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.cpc.spark.qukan.parser.HdfsParser
import javax.sound.sampled.AudioFormat.Encoding
import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * Created by YuntaoMa on 06/06/2018.
  */

object UpdateInstallApp {
  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder()
      .appName("Tag bad uid")
      .enableHiveSupport()
      .getOrCreate()


    val cal = Calendar.getInstance()
    //cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val stmt =
      """
        |select trace_op1, trace_op2, trace_op3 from dl_cpc.cpc_all_trace_log where `date` = "%s" and trace_type = "%s"
      """.stripMargin.format(date, "app_list")
    println(stmt)
    val all_list = spark.sql(stmt).rdd.take(10).map {
      r =>
        val op_type = r.getAs[String](0)
        val did = r.getAs[String](1)
        val in_b64 = r.getAs[String](2)
        var in : String = ""
        var apps = Seq[(String, String)]()
        if (in_b64 != null) {
          val in_gzip = com.cpc.spark.streaming.tools.Encoding.base64Decoder(in_b64).toArray
          in = Gzip.decompress(in_gzip) match {
            case Some(s) => s
            case None => null
          }
          if (in != null) {
              apps = for {
              JArray(pkgs) <- parse(in)
              JObject(pkg) <- pkgs
              JField("name", JString(name)) <- pkg
              JField("package_name", JString(package_name)) <- pkg
              p = (name, package_name)
            } yield p
          }
        }
        (op_type, did, apps)
    }

    all_list.take(20).foreach(println)



  }

}