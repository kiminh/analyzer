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
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.format(cal.getTime)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val minute = cal.get(Calendar.MINUTE)

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


    println(reponse.asBytes.code)
    val data = reponse.asBytes.body.drop(16)

    println(data.length)

    val idxItems = idxinterface.Idx.IdxItems.parseFrom(data)

    val gitemsCount = idxItems.getGitemsCount
    val ditemsCount = idxItems.getDitemsCount
    println("count: " + gitemsCount + ", ditemsCount")

    /*
        var ideaItemMap = Map[Int, Idea]()
        var unitItemMap = Map[Int, Group]()
        var idx = Seq[Group]()

        for (i <- 0 until gitemsCount) {
          val dItem = idxItems.getDitems(i) //ideaItem

          val ideaid = dItem.getIdeaid
          val idea = GetItem.getIdea(dItem)
          ideaItemMap += (ideaid -> idea)
        }

        for (i <- 0 until ditemsCount) {
          val gItems = idxItems.getGitems(i) //groupItem

          val unitid = GetItem.getGroup(gItems)
          unitid.foreach { u =>
            val ideaid = u.ideaid
            unitItemMap += (ideaid -> u)
          }
        }
        println("unitItemMap count:  " + unitItemMap.size, "head:" + unitItemMap.head)
        println("ideaItemMap count:  " + ideaItemMap.size, "head:" + ideaItemMap.head)


        unitItemMap.foreach { u =>
          val uIdeaid = u._1
          val unitItem = u._2
          ideaItemMap.foreach { i =>
            val iIdeaid = i._1
            val ideaItem = i._2
            if (uIdeaid == iIdeaid) {
              unitItem.copy(mtype = ideaItem.mtype,
                width = ideaItem.width,
                height = ideaItem.height,
                interaction = ideaItem.interaction,
                `class` = ideaItem.`class`,
                material_level = ideaItem.material_level,
                siteid = ideaItem.siteid,
                white_user_ad_corner = ideaItem.white_user_ad_corner,
                date = date,
                hour = hour.toString,
                minute = minute.toString,
                timestamp = timestamp)
            }
            idx :+= unitItem
          }
        }
        println("idx count:  " + idx.size, "head:" + idx.head)

        val idxRDD = spark.sparkContext.parallelize(idx)
        spark.createDataFrame(idx)
          .repartition(1)
          .write
          .mode("overwrite")
          .parquet(s"hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_ad_index/date=$date/hour=$hour/minute=$minute")

        spark.sql(
          s"""
             |alter table dl_cpc.xx if not exists add partitions(date = "$date",hour="$hour",minute="$minute")
             |location 'hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_ad_index/date=$date/hour=$hour/minute=$minute'
           """.stripMargin)
    */
    println("done.")

  }


}

case class Idx(
                var timestamp: Long = 0,
                var date: String = "",
                var hour: String = "",
                var minute: String = "",
                var ideaid: Int = 0,
                var unitid: Int = 0,
                var planid: Int = 0,
                var userid: Int = 0,
                var price: Int = 0,
                var charge_type: Int = 0,
                var regionals: String = "",
                var os: Int = 0,
                var adslot_type: Int = 0,
                var timepub: String = "",
                var blacksid: String = "",
                var whiteid: String = "",
                var ages: String = "",
                var gender: Int = 0,
                var freq: Int = 0,
                var user_level: Int = 0,
                var user_type: Int = 0,
                var interests: String = "",
                var media_class: String = "",
                var phonelevel: String = "",
                var cvr_threshold: Int = 0,
                var balance: Int = 0,
                var new_user: Int = 0,
                var user_weight: Int = 0,
                var network: String = "",
                var black_install_pkg: String = "",
                var dislikes: String = "",
                var click_freq: Int = 0,
                var white_install_pkg: String = "",
                var student: Int = 0,
                var isocpc: Int = 0,
                var content_category: String = "",
                var ocpc_price: Int = 0,
                var ocpc_bid_update_time: Int = 0,
                var conversion_goal: Int = 0,
                var target_uids: String = "",
                var delivery_type: Int = 0,
                var adslot_weight: String = "",
                var target_adslot_ids: String = "",
                var mtype: Int = 0,
                var width: Int = 0,
                var height: Int = 0,
                var interaction: Int = 0,
                var `class`: Int = 0,
                var material_level: Int = 0,
                var siteid: Int = 0,
                var white_user_ad_corner: Int = 0,
                var ext_int: collection.Map[String, Int] = null,
                var ext_string: collection.Map[String, String] = null
              )
