package com.cpc.spark.adindex

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

object AdvertisingIndex {
  def main(args: Array[String]): Unit = {
    val url = "http://192.168.80.229:9090/reqdumps?filename=index.dump&hostname=dumper&fileMd5=1"

    val client = new HttpClient
    val method = new GetMethod(url)
    client.executeMethod(method)

    println(method.getStatusLine)

//    val body = method.getResponseBodyAsString
//    val data = body.substring(16)
//    val idxItems = Idx.IdxItems.parseFrom(data.getBytes)
//    val gitemsCount = idxItems.getGitemsCount
//    val ditemsCount = idxItems.getDitemsCount
//
//    /*
//    for循环
//     */
//    val idx = Seq[Idx]()
//    for (i <- 0 until gitemsCount; j <- 0 until ditemsCount) {
//      val ideaItem = idxItems.getDitems(i)
//      idxItems.getDitemsList
//
//      val unitItem = idxItems.getGitems(i)
//    }


  }


}

case class Idx(
                var timestamp: Int = 0,
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
