package com.cpc.spark.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import scalaj.http.Http

import scala.util.Random

/**
  * 从算法中心提供的接口中获取阅读特征存到hive表中
  * 以每天获取的新增文章id去查询redis（只有安卓用户）
  *
  * 读取 文章id→ keyword关键词
  * '{"DocId":[118870195],"GroupId":8}
  *
  * @author zhy
  * @version 1.0
  *          2018-09-21
  */

object ArticlesFeatures {
  private val urls = Seq(
    "http://172.16.55.193:5020/get/doc_feature",
    "http://172.16.55.194:5020/get/doc_feature"
  )

  def main(args: Array[String]): Unit = {

    val date = args(0)

    //val format = new SimpleDateFormat("yyyy-MM-dd")
    //val cal = Calendar.getInstance()
    //cal.setTime(format.parse(date))
    //cal.add(Calendar.DATE, -7)
    //val last7day = format.format(cal.getTime)

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sql =
      s"""
         | select distinct id, title, detail
         | from gobblin.qukan_content
         | where to_date(create_time) = '$date'
      """.stripMargin
    println("sql" + sql)

    spark.sql(sql)
      .repartition(20)
      .rdd
      .map {
        x =>
          val doc_id = x.getAs[Long]("id")
          val title = x.getAs[String]("title")
          val detail = x.getAs[String]("detail")
          (doc_id, title, detail, getDocFeature(doc_id))
      }
      .filter(_._4 != null)
      .map { x =>
        (x._1, x._2, x._3, (for (i <- x._4) yield i.featureName -> i.featureValue).toMap)
      }
      .toDF("doc_id", "title", "detail", "features")
      .write.mode("overwrite")
      .orc(s"/warehouse/dl_cpc.db/cpc_doc_features_from_algo/load_date=$date")

    spark.sql(
      s"""
         | alter table dl_cpc.cpc_doc_features_from_algo add if not exists partition(load_date='$date')
         | location "/warehouse/dl_cpc.db/cpc_doc_features_from_algo/load_date=$date"
      """.stripMargin)

    println("get features about articles reading from algorithm center done !")
  }

  def getDocFeature(docId: Long): Array[Feature] = {
    val id = Random.nextInt(2)
    val data = "{\"GroupId\":8,\"DocId\":[\"" + docId + "\"]}"

    val result = Http(urls(id)).postData(data).asString

    try {
      val a = JSON.parseObject[DocFeatures](result.body, classOf[DocFeatures])
      println("~~~~~:" + a.features(0).feature)
      a.features(0).feature

    } catch {
      case _: Exception => null
    }
  }


  case class FeatureValue(floatValue: Double = 0D,
                          stringValue: String = "",
                          stringArrayValue: Array[String] = Array())

  case class Feature(featureName: String = "", featureValue: FeatureValue = null)

  case class DocIdFeature(docId: Long, feature: Array[Feature])

  case class DocFeatures(errcode: Int, features: Array[DocIdFeature])

  //case class Tbl(device_id: String, member_id: String, features: Map[String, FeatureValue])

  /*
    def main(args: Array[String]): Unit = {
      val id = Random.nextInt(2)
      val data = "{\"GroupId\":8,\"Uid\":[\"" + "68918312" + "\"]}"

      val result = Http(urls(id)).postData(data).asString
      val a = JSON.parseObject[UserFeatures](result.body, classOf[UserFeatures])
      println(a.features(0).feature(1).featureName)
    }*/

}
