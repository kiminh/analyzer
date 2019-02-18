package com.cpc.spark.adindex

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import scalaj.http.Http

object AdvertisingIndex {
  def main(args: Array[String]): Unit = {
    val url = "http://192.168.80.229:9090/reqdumps?filename=index.dump&hostname=dumper&fileMd5=1"

    val spark = SparkSession.builder()
      .appName(" ad index table to hive")
      .enableHiveSupport()
      .getOrCreate()


    //获取当前时间

    val cal = Calendar.getInstance()
    val timestamp = (cal.getTimeInMillis / 1000).toInt
    val date_format = new SimpleDateFormat("yyyy-MM-dd")
    val hour_format = new SimpleDateFormat("HH")
    val min_format = new SimpleDateFormat("mm")
    val date = date_format.format(cal.getTime)
    val hour = hour_format.format(cal.getTime)
    val min = min_format.format(cal.getTime)

    println(timestamp, date, hour, min)

    //    val client = new HttpClient
    //    val method = new GetMethod(url)
    //    val response=client.executeMethod(method)
    //    println("status:" + method.getStatusLine.getStatusCode)
    //
    //    val body = method.getResponseBodyAsString
    //    method.releaseConnection()
    //    val data = body.substring(15)
    val reponse = Http(url)
      .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
      .asBytes

    println(reponse.code)
    var data = Array[Byte]()
    if (reponse.code == 200) {
      data = reponse.body.drop(16)
    }


    println(data.length)

    val idxItems = idxinterface.Idx.IdxItems.parseFrom(data)

    val gitemsCount = idxItems.getGitemsCount
    val ditemsCount = idxItems.getDitemsCount
    println("count: " + gitemsCount + ", ditemsCount: " + ditemsCount)


    var ideaItemSeq = Seq[Idea]()
    var unitItemSeq = Seq[Group]()
    var idx = Seq[Group]()

    for (i <- 0 until ditemsCount) {
      val dItem = idxItems.getDitems(i) //ideaItem

      val idea = GetItem.getIdea(dItem)
      ideaItemSeq :+= idea
    }

    for (i <- 0 until gitemsCount) {
      val gItems = idxItems.getGitems(i) //groupItem

      val unitid = GetItem.getGroup(gItems)
      unitid.foreach { u =>
        unitItemSeq :+= u
      }
    }

    println("unitItemSeq count:  " + unitItemSeq.size, "head:" + unitItemSeq.head)
    println("ideaItemSeq count:  " + ideaItemSeq.size, "head:" + ideaItemSeq.head)

    for (i <- unitItemSeq) {
      if (i.ideaid != 0) {
        println(i.ideaid, i)
      }
    }

    for (u <- unitItemSeq;
         i <- ideaItemSeq) {
      var unitItem = u
      if (u.ideaid == i.ideaid) {
        unitItem = unitItem.copy(
          mtype = i.mtype,
          width = i.width,
          height = i.height,
          interaction = i.interaction,
          `class` = i.`class`,
          material_level = i.material_level,
          siteid = i.siteid,
          white_user_ad_corner = i.white_user_ad_corner,
          timestamp = timestamp)

        idx :+= u
      }
    }


    println("idx count:  " + idx.size, "head:" + idx.head)

    val idxRDD = spark.sparkContext.parallelize(idx)
    spark.createDataFrame(idx)
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_ad_index/date=$date/hour=$hour/minute=$min")
    println(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_ad_index/date=$date/hour=$hour/minute=$min")
    spark.sql(
      s"""
         |alter table dl_cpc.cpc_ad_index add if not exists partition(date = "$date",hour="$hour",minute="$min")
         |location 'hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_ad_index/date=$date/hour=$hour/minute=$min'
           """.stripMargin)
    spark.close()

    println("done.")

  }


}
