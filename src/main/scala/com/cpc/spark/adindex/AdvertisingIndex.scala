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


    val reponse = Http(url)
      .timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
      .asBytes

    println(reponse.code)
    var data = Array[Byte]()
    if (reponse.code != 200) {
      println("reponse code != 200, 没有成功下载到文件")
      System.exit(1)
    }

    data = reponse.body.drop(16)
    println(data.length)

    val idxItems = idxinterface.Idx.IdxItems.parseFrom(data)

    val gitemsCount = idxItems.getGitemsCount
    val ditemsCount = idxItems.getDitemsCount
    println("count: " + gitemsCount + ", ditemsCount: " + ditemsCount)


    var ideaItemMap = Map[Int, Idea]()
    var unitItemSeq = Seq[Group]()
    var idx = Seq[Group]()

    for (i <- 0 until ditemsCount) {
      val dItem = idxItems.getDitems(i) //ideaItem

      val ideaItem = GetItem.getIdea(dItem)
      ideaItemMap += (ideaItem.ideaid -> ideaItem)
    }

    for (i <- 0 until gitemsCount) {
      val gItems = idxItems.getGitems(i) //groupItem

      val unitItem = GetItem.getGroup(gItems)
      unitItem.foreach { u =>
        unitItemSeq :+= u
      }
    }

    println("unitItemSeq count:  " + unitItemSeq.size, "head:" + unitItemSeq.head)
    println("ideaItemMap count:  " + ideaItemMap.size, "head:" + ideaItemMap.head)


    for (u <- unitItemSeq) {
      var unitItem = u
      val ideaid = u.ideaid
      if (ideaItemMap.contains(ideaid)) {
        val ideaItem = ideaItemMap(ideaid)
        var extInt = unitItem.ext_int
        extInt = extInt.updated("is_api_callback", ideaItem.is_api_callback)

        unitItem = unitItem.copy(
          mtype = ideaItem.mtype,
          width = ideaItem.width,
          height = ideaItem.height,
          interaction = ideaItem.interaction,
          `class` = ideaItem.`class`,
          material_level = ideaItem.material_level,
          siteid = ideaItem.siteid,
          white_user_ad_corner = ideaItem.white_user_ad_corner,
          timestamp = timestamp,
          ext_int = extInt
        )

        idx :+= unitItem
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
