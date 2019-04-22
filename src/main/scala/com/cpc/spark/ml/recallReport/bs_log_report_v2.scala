package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.Calendar
import bslog.Bslog.NoticeLogBody
import com.cpc.spark.streaming.tools.Encoding
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import collection.mutable._

object bs_log_report_v2 {
  def main(args: Array[String]): Unit = {
    //val beforeNdays = args(0).toInt
    val spark = SparkSession.builder()
      .appName("bs log report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    val stmt: String =
      s"""
        |select trim(split(raw, '\\\\*')[1]) as raw from dl_cpc.cpc_basedata_recall_log
        |where day='$tardate' and length(trim(split(raw, '\\\\*')[1]))>0
      """.stripMargin
    val excp = spark.sparkContext.longAccumulator
    val pbData = spark.sql(stmt).rdd.map{
      r =>
        val pb = r.getAs[String]("raw")
        try{
          val up = NoticeLogBody.parseFrom(Encoding.base64Decoder(pb).toArray).toBuilder
          val exptags = up.getExptagsList.toString
          val searchid = up.getSearchid
          val involved_group_num = up.getGroupStats.getInvolvedGroupNum
          val group_media_num = up.getGroupStats.getGroupMediaNum
          val group_region_num = up.getGroupStats.getGroupRegionNum
          val group_l_v_num = up.getGroupStats.getGroupLVNum
          val group_os_type_num = up.getGroupStats.getGroupOsTypeNum
          val group_p_l_num = up.getGroupStats.getGroupPLNum
          val group_dislike_num = up.getGroupStats.getGroupDislikeNum
          val group_interest_num = up.getGroupStats.getGroupInterestNum
          val group_student_num = up.getGroupStats.getGroupStudentNum
          val group_acc_user_type_num = up.getGroupStats.getGroupAccUserTypeNum
          val group_new_user_num = up.getGroupStats.getGroupNewUserNum
          val group_content_category_num = up.getGroupStats.getGroupContentCategoryNum
          val group_black_install_pkg_num = up.getGroupStats.getGroupBlackInstallPkgNum
          val group_white_install_pkg_num = up.getGroupStats.getGroupWhiteInstallPkgNum
          val group_show_count_num = up.getGroupStats.getGroupShowCountNum
          val group_click_count_num = up.getGroupStats.getGroupClickCountNum
          val matched_group_num = up.getGroupStats.getMatchedGroupNum
          val len_groups = up.getGroupStats.getLenGroups
          val involved_idea_num = up.getInvolvedIdeaNum
          val matched_idea_num = up.getMatchedIdeaNum
          val rnd_idea_num = up.getRndIdeaNum
          val adslot_type = up.getSearchCond.getAdSlotStyle
          val req_io_time = up.getReqIoTime
          val process_time = up.getProcessTime
          val ad_slot_id = up.getSearchCond.getAdSlotId
          val media_class = up.getSearchCond.getMediaClassList.toString
          val hostname = up.getHostname
          val group_age_num = up.getGroupStats.getGroupAgeNum
          val group_gender_num = up.getGroupStats.getGroupGenderNum
          val group_network_num = up.getGroupStats.getGroupNetworkNum
          val group_ad_slot_type_num = up.getGroupStats.getGroupAdSlotTypeNum
          val group_map_match_count_num = up.getGroupStats.getGroupMapMatchCountNum
          val groups_hit_media_ids = up.getGroupStats.getGroupsHitMediaIdsList.asScala.toList
          val groups_hit_age_ids = up.getGroupStats.getGroupsHitAgeIdsList.asScala.toList
          val groups_hit_gender_ids = up.getGroupStats.getGroupsHitGenderIdsList.asScala.toList
          val groups_hit_net_work_ids = up.getGroupStats.getGroupsHitNetWorkIdsList.asScala.toList
          val groups_hit_ad_slot_type_ids =  up.getGroupStats.getGroupsHitAdSlotTypeIdsList.asScala.toList
          val groups_hit_media_class_ids =  up.getGroupStats.getGroupsHitMediaClassIdsList.asScala.toList
          val groups_hit_regional_ids = up.getGroupStats.getGroupsHitRegionalIdsList.asScala.toList
          val groups_hit_user_level_ids = up.getGroupStats.getGroupsHitUserLevelIdsList.asScala.toList
          val groups_hit_phone_level_ids = up.getGroupStats.getGroupsHitPhoneLevelIdsList.asScala.toList
          val groups_hit_os_type_ids = up.getGroupStats.getGroupsHitOsTypeIdsList.asScala.toList
          val groups_hit_black_install_pkg_ids = up.getGroupStats.getGroupsHitBlackInstallPkgIdsList.asScala.toList
          val groups_hit_white_install_pkg_ids = up.getGroupStats.getGroupsHitWhiteInstallPkgIdsList.asScala.toList
          val groups_hit_content_category_ids = up.getGroupStats.getGroupsHitContentCategoryIdsList.asScala.toList
          val groups_hit_new_user_ids = up.getGroupStats.getGroupsHitNewUserIdsList.asScala.toList
          val groups_hit_acc_user_type_ids = up.getGroupStats.getGroupsHitAccUserTypeIdsList.asScala.toList


          BsLog1(
            searchid=searchid,
            involved_group_num=involved_group_num.toInt,
            group_media_num=group_media_num.toInt,
            group_region_num=group_region_num.toInt,
            group_l_v_num=group_l_v_num.toInt,
            group_os_type_num=group_os_type_num.toInt,
            group_p_l_num=group_p_l_num.toInt,
            group_dislike_num=group_dislike_num.toInt,
            group_interest_num=group_interest_num.toInt,
            group_student_num=group_student_num.toInt,
            group_acc_user_type_num=group_acc_user_type_num.toInt,
            group_new_user_num=group_new_user_num.toInt,
            group_content_category_num=group_content_category_num.toInt,
            group_black_install_pkg_num=group_black_install_pkg_num.toInt,
            group_white_install_pkg_num=group_white_install_pkg_num.toInt,
            group_show_count_num=group_show_count_num.toInt,
            group_click_count_num=group_click_count_num.toInt,
            matched_group_num=matched_group_num.toInt,
            len_groups=len_groups.toInt,
            involved_idea_num=involved_idea_num.toInt,
            matched_idea_num=matched_idea_num.toInt,
            rnd_idea_num=rnd_idea_num.toInt,
            exptags=exptags.substring(1, exptags.length()-1),
            adslot_type=adslot_type.toString,
            req_io_time=req_io_time.toInt,
            process_time=process_time.toInt,
            ad_slot_id=ad_slot_id.toString,
            media_class=media_class.substring(1, media_class.length()-1),
            hostname=hostname.toString,
            group_age_num = group_age_num,
            group_gender_num = group_gender_num,
            group_network_num = group_network_num,
            group_ad_slot_type_num = group_ad_slot_type_num,
            group_map_match_count_num = group_map_match_count_num,
            groups_hit_media_ids = groups_hit_media_ids,
            groups_hit_age_ids = groups_hit_age_ids,
            groups_hit_gender_ids = groups_hit_gender_ids,
            groups_hit_net_work_ids = groups_hit_net_work_ids,
            groups_hit_ad_slot_type_ids =  groups_hit_ad_slot_type_ids,
            groups_hit_media_class_ids =  groups_hit_media_class_ids,
            groups_hit_regional_ids = groups_hit_regional_ids,
            groups_hit_user_level_ids = groups_hit_user_level_ids,
            groups_hit_phone_level_ids =  groups_hit_phone_level_ids,
            groups_hit_os_type_ids = groups_hit_os_type_ids,
            groups_hit_black_install_pkg_ids = groups_hit_black_install_pkg_ids,
            groups_hit_white_install_pkg_ids = groups_hit_white_install_pkg_ids,
            groups_hit_content_category_ids = groups_hit_content_category_ids,
            groups_hit_new_user_ids = groups_hit_new_user_ids,
            groups_hit_acc_user_type_ids = groups_hit_acc_user_type_ids
          )
        } catch {
          case ex: Exception => excp.add(1); null
        }

    }.filter(_ != null).toDF("searchid", "involved_group_num", "group_media_num", "group_region_num", "group_l_v_num", "group_os_type_num",
      "group_p_l_num", "group_dislike_num", "group_interest_num", "group_student_num", "group_acc_user_type_num", "group_new_user_num",
      "group_content_category_num", "group_black_install_pkg_num", "group_white_install_pkg_num", "group_show_count_num",
      "group_click_count_num","matched_group_num", "len_groups",
      "involved_idea_num", "matched_idea_num", "rnd_idea_num", "exptags", "adslot_type", "req_io_time", "process_time",
      "ad_slot_id", "media_class", "hostname","group_age_num","group_gender_num","group_network_num", "group_ad_slot_type_num",
      "group_map_match_count_num","groups_hit_media_ids","groups_hit_age_ids","groups_hit_gender_ids", "groups_hit_net_work_ids",
      "groups_hit_ad_slot_type_ids","groups_hit_media_class_ids", "groups_hit_regional_ids","groups_hit_user_level_ids",
      "groups_hit_phone_level_ids","groups_hit_os_type_ids", "groups_hit_black_install_pkg_ids","groups_hit_white_install_pkg_ids",
      "groups_hit_content_category_ids", "groups_hit_new_user_ids","groups_hit_acc_user_type_ids")
//    pbData.createOrReplaceTempView("temp_table")
//    println(excp.value)
//    val insertIntoTable =
//      s"""
//         |insert overwrite table dl_cpc.recall_filter_number_report_v2 partition (`date`='$tardate')
//         |select * from temp_table
//      """.stripMargin
//    spark.sql(insertIntoTable)
    pbData.repartition(1000).write.mode("overwrite").saveAsTable("test.bslog")
  }
  case class BsLog1(
                     var searchid: String="",
                     var involved_group_num :Int=0,
                     var group_media_num:Int=0,
                     var group_region_num:Int=0,
                     var group_l_v_num:Int=0,
                     var group_os_type_num:Int=0,
                     var group_p_l_num:Int=0,
                     var group_dislike_num:Int=0,
                     var group_interest_num:Int=0,
                     var group_student_num:Int=0,
                     var group_acc_user_type_num:Int=0,
                     var group_new_user_num:Int=0,
                     var group_content_category_num:Int=0,
                     var group_black_install_pkg_num:Int=0,
                     var group_white_install_pkg_num:Int=0,
                     var group_show_count_num:Int=0,
                     var group_click_count_num:Int=0,
                     var matched_group_num:Int=0,
                     var len_groups:Int=0,
                     var involved_idea_num:Int=0,
                     var matched_idea_num:Int=0,
                     var rnd_idea_num:Int=0,
                     var exptags: String="",
                     var adslot_type: String="",
                     var req_io_time: Int=0,
                     var process_time: Int=0,
                     var ad_slot_id: String="",
                     var media_class: String="",
                     var hostname: String="",
                     var group_age_num: Int=0,
                     var group_gender_num: Int=0,
                     var group_network_num: Int=0,
                     var group_ad_slot_type_num : Int=0,
                     var group_map_match_count_num: Int=0,
                     var groups_hit_media_ids: List[Integer]=List(),
                     var groups_hit_age_ids: List[Integer]=List(),
                     var groups_hit_gender_ids: List[Integer]=List(),
                     var groups_hit_net_work_ids: List[Integer]=List(),
                     var groups_hit_ad_slot_type_ids: List[Integer]=List(),
                     var groups_hit_media_class_ids: List[Integer]=List(),
                     var groups_hit_regional_ids: List[Integer]=List(),
                     var groups_hit_user_level_ids: List[Integer]=List(),
                     var groups_hit_phone_level_ids: List[Integer]=List(),
                     var groups_hit_os_type_ids: List[Integer]=List(),
                     var groups_hit_black_install_pkg_ids: List[Integer]=List(),
                     var groups_hit_white_install_pkg_ids: List[Integer]=List(),
                     var groups_hit_content_category_ids: List[Integer]=List(),
                     var groups_hit_new_user_ids: List[Integer]=List(),
                     var groups_hit_acc_user_type_ids: List[Integer]=List()
                   )
}
