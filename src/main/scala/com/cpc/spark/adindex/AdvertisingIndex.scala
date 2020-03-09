package com.cpc.spark.adindex

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import idxinterface.Idx
import org.apache.spark.sql.{SaveMode, SparkSession}
import scalaj.http.Http

object AdvertisingIndex {

  val URL_TOGO: String = "http://192.168.80.229:9090/reqdumps?filename=index.dump&hostname=dumper&fileMd5=1"
  val DATE_FMT: String = "yyyy-MM-dd HH:mm:ss"
  val DAY_FMT: String = "yyyy-MM-dd"
  val HH_FMT: String = "HH"
  val MM_FMT: String = "mm"
  val RPT_MIN: Int = 1

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("[cpc-infra] ad-index").enableHiveSupport().getOrCreate()
    val cal: Calendar = getCalendarByDateTimeStr(args(0).trim)
    val (date, hour, minute) = (new SimpleDateFormat(DAY_FMT).format(cal.getTime), new SimpleDateFormat(HH_FMT).format(cal.getTime), new SimpleDateFormat(MM_FMT).format(cal.getTime))

    val idxItems: Idx.IdxItems = idxinterface.Idx.IdxItems.parseFrom(Http(URL_TOGO).timeout(connTimeoutMs = 40000, readTimeoutMs = 40000).asBytes.body.drop(16))
    val ideaItemMap: Map[Int, Idea] = (0 until idxItems.getDitemsCount).map { i: Int => GetItem.getIdea(idxItems.getDitems(i)).ideaid -> GetItem.getIdea(idxItems.getDitems(i))}.toMap
    val unitItemSeq: Seq[Group] = (0 until idxItems.getGitemsCount).flatMap { i: Int => GetItem.getGroup(idxItems.getGitems(i)) }
    val idx: Seq[Group] = unitItemSeq.map((u: Group) => {
      if (ideaItemMap.contains(u.ideaid)) {
        val ideaItem: Idea = ideaItemMap(u.ideaid)
        u.copy(
          mtype = ideaItem.mtype,
          width = ideaItem.width,
          height = ideaItem.height,
          interaction = ideaItem.interaction,
          `class` = ideaItem.`class`,
          material_level = ideaItem.material_level,
          siteid = ideaItem.siteid,
          white_user_ad_corner = ideaItem.white_user_ad_corner,
          timestamp = (cal.getTimeInMillis / 1000L).toInt,
          ext_int = u.ext_int.updated("is_api_callback", ideaItem.is_api_callback)
        )
      } else null
    }).filter((u: Group) => u != null)

    val location: String = s"hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_ad_index/date=$date/hour=$hour/minute=$minute"
    spark.createDataFrame(idx).repartition(RPT_MIN).write.mode(SaveMode.Overwrite).parquet(location)
    spark.sql(s"""alter table dl_cpc.cpc_ad_index add if not exists partition (date = "$date", hour="$hour", minute="$minute") location '$location'""")
  }

  def getCalendarByDateTimeStr(dateTimeStr: String): Calendar = {
    val dateTime: Date = new SimpleDateFormat(DATE_FMT).parse(dateTimeStr)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dateTime)
    cal
  }
}
