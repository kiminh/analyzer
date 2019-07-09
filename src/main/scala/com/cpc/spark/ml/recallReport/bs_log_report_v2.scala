package com.cpc.spark.ml.recallReport

import bslog.Bslog.NoticeLogBody
import com.cpc.spark.streaming.tools.Encoding
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

object bs_log_report_v2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bs log report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val tardate = args(0)
    val hour = args(1)
    val excp = spark.sparkContext.longAccumulator

    val stmt: String =
      s"""
        |select trim(split(raw, '\\\\*')[1]) as raw from dl_cpc.cpc_basedata_recall_log
        |where day='$tardate' and hour='$hour' and length(trim(split(raw, '\\\\*')[1]))>0
      """.stripMargin
    println(stmt)
    val pbData = spark.sql(stmt).rdd.map {
      r =>
        val pb = r.getAs[String]("raw")
        try {
          val up = NoticeLogBody.parseFrom(Encoding.base64Decoder(pb).toArray).toBuilder
          val exptags = up.getExptagsList.toString
          val searchid = up.getSearchid
          val group_media_num = up.getGroupStats.getGroupMediaNum
          val group_region_num = up.getGroupStats.getGroupRegionNum
          val group_l_v_num = up.getGroupStats.getGroupLVNum
          val group_os_type_num = up.getGroupStats.getGroupOsTypeNum
          val group_p_l_num = up.getGroupStats.getGroupPLNum
          val group_interest_num = up.getGroupStats.getGroupInterestNum
          val group_acc_user_type_num = up.getGroupStats.getGroupAccUserTypeNum
          val group_new_user_num = up.getGroupStats.getGroupNewUserNum
          val group_content_category_num = up.getGroupStats.getGroupContentCategoryNum
          val group_black_install_pkg_num = up.getGroupStats.getGroupBlackInstallPkgNum
          val group_white_install_pkg_num = up.getGroupStats.getGroupWhiteInstallPkgNum
          val group_show_count_num = up.getGroupStats.getGroupShowCountNum
          val group_click_count_num = up.getGroupStats.getGroupClickCountNum
          val matched_group_num = up.getGroupStats.getMatchedGroupNum
          val involved_group_num = up.getGroupStats.getInvolvedGroupNum
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
          val groups_hit_media_ids = up.getGroupStats.getGroupsHitMediaIdsList.asScala.toList.mkString(",")
          val groups_hit_age_ids = up.getGroupStats.getGroupsHitAgeIdsList.asScala.toList.mkString(",")
          val groups_hit_gender_ids = up.getGroupStats.getGroupsHitGenderIdsList.asScala.toList.mkString(",")
          val groups_hit_net_work_ids = up.getGroupStats.getGroupsHitNetWorkIdsList.asScala.toList.mkString(",")
          val groups_hit_ad_slot_type_ids = up.getGroupStats.getGroupsHitAdSlotTypeIdsList.asScala.toList.mkString(",")
          val groups_hit_media_class_ids = up.getGroupStats.getGroupsHitMediaClassIdsList.asScala.toList.mkString(",")
          val groups_hit_regional_ids = up.getGroupStats.getGroupsHitRegionalIdsList.asScala.toList.mkString(",")
          val groups_hit_user_level_ids = up.getGroupStats.getGroupsHitUserLevelIdsList.asScala.toList.mkString(",")
          val groups_hit_phone_level_ids = up.getGroupStats.getGroupsHitPhoneLevelIdsList.asScala.toList.mkString(",")
          val groups_hit_os_type_ids = up.getGroupStats.getGroupsHitOsTypeIdsList.asScala.toList.mkString(",")
          val groups_hit_black_install_pkg_ids = up.getGroupStats.getGroupsHitBlackInstallPkgIdsList.asScala.toList.mkString(",")
          val groups_hit_white_install_pkg_ids = up.getGroupStats.getGroupsHitWhiteInstallPkgIdsList.asScala.toList.mkString(",")
          val groups_hit_content_category_ids = up.getGroupStats.getGroupsHitContentCategoryIdsList.asScala.toList.mkString(",")
          val groups_hit_new_user_ids = up.getGroupStats.getGroupsHitNewUserIdsList.asScala.toList.mkString(",")
          val groups_hit_acc_user_type_ids = up.getGroupStats.getGroupsHitAccUserTypeIdsList.asScala.toList.mkString(",")
          val groups_hit_interest_or_user_signal_ids = up.getGroupStats.getGroupsHitInterestOrUserSignalIdsList.asScala.toList.mkString(",")
          val groups_filtered_by_ad_show_ids = up.getGroupStats.getGroupsFilteredByAdShowIdsList.asScala.toList.mkString(",")
          val groups_filtered_by_ad_click_ids = up.getGroupStats.getGroupsFilteredByAdClickIdsList.asScala.toList.mkString(",")
          val groups_filtered_by_black_user_ids = up.getGroupStats.getGroupsFilteredByBlackUserIdsList.asScala.toList.mkString(",")
          val groups_filtered_by_black_sid_ids = up.getGroupStats.getGroupsFilteredByBlackSidIdsList.asScala.toList.mkString(",")
          val groups_filtered_by_not_delivery_pr_ids = up.getGroupStats.getGroupsFilteredByNotDeliveryPrIdsList.asScala.toList.mkString(",")

          val ideas_filtered_by_material_type_ids = up.getIdeaStats.getIdeasFilteredByMaterialTypeList.asScala.toList.mkString(",")
          val ideas_filtered_by_interaction_ids = up.getIdeaStats.getIdeasFilteredByInteractionList.asScala.toList.mkString(",")
          val ideas_filtered_by_black_class_ids = up.getIdeaStats.getIdeasFilteredByBlackClassList.asScala.toList.mkString(",")
          val ideas_filtered_by_acc_class_ids = up.getIdeaStats.getIdeasFilteredByAccClassList.asScala.toList.mkString(",")
          val ideas_filtered_by_material_level_ids = up.getIdeaStats.getIdeasFilteredByMaterialLevelList.asScala.toList.mkString(",")
          val ideas_filtered_by_only_site_ids = up.getIdeaStats.getIdeasFilteredByOnlySiteList.asScala.toList.mkString(",")
          val ideas_filtered_by_filter_goods_ids = up.getIdeaStats.getIdeasFilteredByFilterGoodsList.asScala.toList.mkString(",")
          val ideas_filtered_by_pkg_filter_ids = up.getIdeaStats.getIdeasFilteredByPkgFilterList.asScala.toList.mkString(",")
          val ideas_filtered_by_chitu_ids = up.getIdeaStats.getIdeasFilteredByChituList.asScala.toList.mkString(",")

          val bid_avg_before_filter = up.getBidAvgBeforeFilter
          val bid_avg_after_filter = up.getBidAvgAfterFilter
          val bid_avg_return_as = up.getBidAvgReturnAs

          val ad_num_of_delivery = up.getAdNumOfDelivery

          val media_appsid = up.getSearchCond.getAppsid
          val module_times = up.getModuleTimesList.asScala.toList.mkString(",")
          val groups_hit_ad_slot_ids = up.getGroupStats.getGroupsHitAdSlotIdList.asScala.toList.mkString(",")
          val groups_hit_not_allow_delivery_ids = up.getGroupStats.getGroupsHitNotAllowDeliveryList.asScala.toList.mkString(",")

          val ideas_filtered_by_bidrankcut_ids = up.getIdeaStats.getIdeasFilteredByBidrankcutList.asScala.toList.mkString(",")
          val ideas_filtered_by_middle_page_ids = up.getIdeaStats.getIdeasFilteredByMiddlePageList.asScala.toList.mkString(",")
          val involved_ideas_ids = up.getIdeaStats.getInvolvedIdeasList.asScala.toList.mkString(",")
          val matched_ideas_ids = up.getIdeaStats.getMatchedIdeasList.asScala.toList.mkString(",")
          val rnd_ideas_ids = up.getIdeaStats.getRndIdeasList.asScala.toList.mkString(",")

          val channelid = up.getSearchCond.getChannelid
          val ad_slot_type = up.getSearchCond.getAdSlotStyle.getNumber.toString
          val acc_class = up.getSearchCond.getAccClassList.asScala.toList.mkString(",")
          val black_class = up.getSearchCond.getBlackClassList.asScala.toList.mkString(",")
          val acc_user_type = up.getSearchCond.getAccUserTypeList.asScala.toList.mkString(",")
          val slot_width = up.getSearchCond.getSlotWidth.toString
          val slot_height = up.getSearchCond.getSlotHeight.toString
          val keyword = up.getSearchCond.getKeyword
          val white_material_level = up.getSearchCond.getWhiteMaterialLevelList.asScala.toList.mkString(",")
          val regionals = up.getSearchCond.getRegionalsList.asScala.toList.mkString(",")
          val os_type = up.getSearchCond.getOSType.getNumber.toString
          val age = up.getSearchCond.getAge.toString
          val gender = up.getSearchCond.getGender.toString
          val coin = up.getSearchCond.getCoin.toString
          val interests = up.getSearchCond.getInterestsList.asScala.toList.mkString(",")
          val phone_level = up.getSearchCond.getPhoneLevel.toString
          val adnum = up.getSearchCond.getAdnum.toString
          val new_user = up.getSearchCond.getNewUser.toString
          val network = up.getSearchCond.getNetwork.toString
          val only_site = up.getSearchCond.getOnlySite.toString
          val direct_uid = up.getSearchCond.getDirectUid.toString
          val material_styles_s = up.getSearchCond.getMaterialStylesSList.asScala.toList.map(_.getNumber.toString).mkString(",")
          val interactions_s = up.getSearchCond.getInteractionsSList.asScala.toList.map(_.getNumber.toString).mkString(",")
          val ideas_filtered_by_qjp_antou_qtt = up.getIdeaStats.getIdeasFilteredByQjpAntouQttList.asScala.toList.mkString(",")
          val ideas_filtered_by_unknown = up.getIdeaStats.getIdeasFilteredByUnknownList.asScala.toList.mkString(",")
          val after_rank_ideas = up.getIdeaStats.getAfterRankIdeasList.asScala.toList.mkString(",")
          val ideas_filtered_by_content = up.getIdeaStats.getIdeasFilteredByContentList.asScala.toList.mkString(",")
          val embedding_ctr_uniq_ideas = up.getIdeaStats.getEmbeddingCtrUniqIdeasList.asScala.toList.mkString(",")
          val embedding_redis_hit = up.getEmbeddingRedisHit
          val ideas_filtered_by_fill_freq_control = up.getIdeaStats.getIdeasFilteredByFillFreqControlList.asScala.toList.mkString(",")
          val ideas_filtered_by_show_freq_control = up.getIdeaStats.getIdeasFilteredByShowFreqControlList.asScala.toList.mkString(",")


          if (exptags.contains("bsfilterdetail")) {
            BsLog1(
              searchid = searchid,
              group_media_num = group_media_num.toInt,
              group_region_num = group_region_num.toInt,
              group_l_v_num = group_l_v_num.toInt,
              group_os_type_num = group_os_type_num.toInt,
              group_p_l_num = group_p_l_num.toInt,
              group_interest_num = group_interest_num.toInt,
              group_acc_user_type_num = group_acc_user_type_num.toInt,
              group_new_user_num = group_new_user_num.toInt,
              group_content_category_num = group_content_category_num.toInt,
              group_black_install_pkg_num = group_black_install_pkg_num.toInt,
              group_white_install_pkg_num = group_white_install_pkg_num.toInt,
              group_show_count_num = group_show_count_num.toInt,
              group_click_count_num = group_click_count_num.toInt,
              matched_group_num = matched_group_num.toInt,
              involved_group_num = involved_group_num.toInt,
              len_groups = len_groups.toInt,
              involved_idea_num = involved_idea_num.toInt,
              matched_idea_num = matched_idea_num.toInt,
              rnd_idea_num = rnd_idea_num.toInt,
              exptags = exptags.substring(1, exptags.length() - 1),
              adslot_type = adslot_type.toString,
              req_io_time = req_io_time.toInt,
              process_time = process_time.toInt,
              ad_slot_id = ad_slot_id.toString,
              media_class = media_class.substring(1, media_class.length() - 1),
              hostname = hostname.toString,
              group_age_num = group_age_num,
              group_gender_num = group_gender_num,
              group_network_num = group_network_num,
              group_ad_slot_type_num = group_ad_slot_type_num,
              group_map_match_count_num = group_map_match_count_num,
              groups_hit_media_ids = groups_hit_media_ids,
              groups_hit_age_ids = groups_hit_age_ids,
              groups_hit_gender_ids = groups_hit_gender_ids,
              groups_hit_net_work_ids = groups_hit_net_work_ids,
              groups_hit_ad_slot_type_ids = groups_hit_ad_slot_type_ids,
              groups_hit_media_class_ids = groups_hit_media_class_ids,
              groups_hit_regional_ids = groups_hit_regional_ids,
              groups_hit_user_level_ids = groups_hit_user_level_ids,
              groups_hit_phone_level_ids = groups_hit_phone_level_ids,
              groups_hit_os_type_ids = groups_hit_os_type_ids,
              groups_hit_black_install_pkg_ids = groups_hit_black_install_pkg_ids,
              groups_hit_white_install_pkg_ids = groups_hit_white_install_pkg_ids,
              groups_hit_content_category_ids = groups_hit_content_category_ids,
              groups_hit_new_user_ids = groups_hit_new_user_ids,
              groups_hit_acc_user_type_ids = groups_hit_acc_user_type_ids,
              groups_hit_interest_or_user_signal_ids = groups_hit_interest_or_user_signal_ids,
              groups_filtered_by_ad_show_ids = groups_filtered_by_ad_show_ids,
              groups_filtered_by_ad_click_ids = groups_filtered_by_ad_click_ids,
              groups_filtered_by_black_user_ids = groups_filtered_by_black_user_ids,
              groups_filtered_by_black_sid_ids = groups_filtered_by_black_sid_ids,
              groups_filtered_by_not_delivery_pr_ids = groups_filtered_by_not_delivery_pr_ids,
              ideas_filtered_by_material_type_ids = ideas_filtered_by_material_type_ids,
              ideas_filtered_by_interaction_ids = ideas_filtered_by_interaction_ids,
              ideas_filtered_by_black_class_ids = ideas_filtered_by_black_class_ids,
              ideas_filtered_by_acc_class_ids = ideas_filtered_by_acc_class_ids,
              ideas_filtered_by_material_level_ids = ideas_filtered_by_material_level_ids,
              ideas_filtered_by_only_site_ids = ideas_filtered_by_only_site_ids,
              ideas_filtered_by_filter_goods_ids = ideas_filtered_by_filter_goods_ids,
              ideas_filtered_by_chitu_ids = ideas_filtered_by_chitu_ids,
              ideas_filtered_by_pkg_filter_ids = ideas_filtered_by_pkg_filter_ids,
              bid_avg_before_filter = bid_avg_before_filter.toInt,
              bid_avg_after_filter = bid_avg_after_filter.toInt,
              bid_avg_return_as = bid_avg_return_as.toInt,
              ad_num_of_delivery = ad_num_of_delivery.toInt,
              media_appsid = media_appsid,
              module_times = module_times,
              groups_hit_ad_slot_ids = groups_hit_ad_slot_ids,
              groups_hit_not_allow_delivery_ids = groups_hit_not_allow_delivery_ids,
              ideas_filtered_by_bidrankcut_ids = ideas_filtered_by_bidrankcut_ids,
              ideas_filtered_by_middle_page_ids = ideas_filtered_by_middle_page_ids,
              involved_ideas_ids = involved_ideas_ids,
              matched_ideas_ids = matched_ideas_ids,
              rnd_ideas_ids = rnd_ideas_ids,
              channelid = channelid,
              ad_slot_type = ad_slot_type,
              acc_class = acc_class,
              black_class = black_class,
              slot_width = slot_width,
              slot_height = slot_height,
              keyword = keyword,
              white_material_level = white_material_level,
              regionals = regionals,
              os_type = os_type,
              age = age,
              gender = gender,
              coin = coin,
              interests = interests,
              phone_level = phone_level,
              adnum = adnum,
              new_user = new_user,
              network = network,
              only_site = only_site,
              direct_uid = direct_uid,
              material_styles_s = material_styles_s,
              interactions_s = interactions_s,
              acc_user_type = acc_user_type,
              ideas_filtered_by_qjp_antou_qtt = ideas_filtered_by_qjp_antou_qtt,
              ideas_filtered_by_unknown = ideas_filtered_by_unknown,
              after_rank_ideas = after_rank_ideas,
              ideas_filtered_by_content = ideas_filtered_by_content,
              embedding_ctr_uniq_ideas = embedding_ctr_uniq_ideas,
              embedding_redis_hit = embedding_redis_hit,
              ideas_filtered_by_fill_freq_control = ideas_filtered_by_fill_freq_control,
              ideas_filtered_by_show_freq_control = ideas_filtered_by_show_freq_control
            )
          }
          else null
        } catch {
          case ex: Exception => excp.add(1); null
        }
    } .filter(_ != null).toDF("searchid", "group_media_num", "group_region_num", "group_l_v_num", "group_os_type_num",
      "group_p_l_num", "group_interest_num", "group_acc_user_type_num", "group_new_user_num", "group_content_category_num",
      "group_black_install_pkg_num", "group_white_install_pkg_num",
      "group_show_count_num", "group_click_count_num",
      "matched_group_num", "involved_group_num", "len_groups",
      "involved_idea_num", "matched_idea_num", "rnd_idea_num", "exptags", "adslot_type", "req_io_time", "process_time",
      "ad_slot_id", "media_class", "hostname","group_age_num","group_gender_num","group_network_num", "group_ad_slot_type_num",
      "group_map_match_count_num","groups_hit_media_ids","groups_hit_age_ids","groups_hit_gender_ids", "groups_hit_net_work_ids",
      "groups_hit_ad_slot_type_ids","groups_hit_media_class_ids", "groups_hit_regional_ids","groups_hit_user_level_ids",
      "groups_hit_phone_level_ids","groups_hit_os_type_ids", "groups_hit_black_install_pkg_ids","groups_hit_white_install_pkg_ids",
      "groups_hit_content_category_ids", "groups_hit_new_user_ids","groups_hit_acc_user_type_ids",
      "groups_hit_interest_or_user_signal_ids", "groups_filtered_by_ad_show_ids", "groups_filtered_by_ad_click_ids", "groups_filtered_by_black_user_ids",
      "groups_filtered_by_black_sid_ids", "groups_filtered_by_not_delivery_pr_ids", "ideas_filtered_by_material_type_ids",
      "ideas_filtered_by_interaction_ids", "ideas_filtered_by_black_class_ids", "ideas_filtered_by_acc_class_ids",
      "ideas_filtered_by_material_level_ids", "ideas_filtered_by_only_site_ids", "ideas_filtered_by_filter_goods_ids",
      "ideas_filtered_by_chitu_ids", "ideas_filtered_by_pkg_filter_ids", "bid_avg_before_filter",
      "bid_avg_after_filter", "bid_avg_return_as", "ad_num_of_delivery", "media_appsid",
      "module_times", "groups_hit_ad_slot_ids", "groups_hit_not_allow_delivery_ids", "ideas_filtered_by_bidrankcut_ids",
      "ideas_filtered_by_middle_page_ids", "involved_ideas_ids", "matched_ideas_ids", "rnd_ideas_ids",
      "channelid", "ad_slot_type", "acc_class", "black_class", "slot_width", "slot_height", "keyword",
      "white_material_level", "regionals", "os_type", "age", "gender",  "coin", "interests", "phone_level", "adnum",
      "new_user", "network", "only_site", "direct_uid", "material_styles_s", "interactions_s", "acc_user_type",
      "ideas_filtered_by_qjp_antou_qtt", "ideas_filtered_by_unknown", "after_rank_ideas", "ideas_filtered_by_content",
      "embedding_ctr_uniq_ideas", "embedding_redis_hit", "ideas_filtered_by_fill_freq_control", "ideas_filtered_by_show_freq_control"
    )
      .withColumn("`dt`",lit(s"$tardate"))
      .withColumn("`hour`",lit(s"$hour"))
    pbData.repartition(10).write.mode("overwrite").insertInto("dl_cpc.recall_filter_number_report_v3")
  }

  case class BsLog1(
                     var searchid: String = "",
                     var group_media_num: Int = 0,
                     var group_region_num: Int = 0,
                     var group_l_v_num: Int = 0,
                     var group_os_type_num: Int = 0,
                     var group_p_l_num: Int = 0,
                     var group_interest_num: Int = 0,
                     var group_acc_user_type_num: Int = 0,
                     var group_new_user_num: Int = 0,
                     var group_content_category_num: Int = 0,
                     var group_black_install_pkg_num: Int = 0,
                     var group_white_install_pkg_num: Int = 0,
                     var group_show_count_num: Int = 0,
                     var group_click_count_num: Int = 0,
                     var matched_group_num: Int = 0,
                     var involved_group_num: Int = 0,
                     var len_groups: Int = 0,
                     var involved_idea_num: Int = 0,
                     var matched_idea_num: Int = 0,
                     var rnd_idea_num: Int = 0,
                     var exptags: String = "",
                     var adslot_type: String = "",
                     var req_io_time: Int = 0,
                     var process_time: Int = 0,
                     var ad_slot_id: String = "",
                     var media_class: String = "",
                     var hostname: String = "",
                     var group_age_num: Int = 0,
                     var group_gender_num: Int = 0,
                     var group_network_num: Int = 0,
                     var group_ad_slot_type_num: Int = 0,
                     var group_map_match_count_num: Int = 0,
                     var groups_hit_media_ids: String = "",
                     var groups_hit_age_ids: String = "",
                     var groups_hit_gender_ids: String = "",
                     var groups_hit_net_work_ids: String = "",
                     var groups_hit_ad_slot_type_ids: String = "",
                     var groups_hit_media_class_ids: String = "",
                     var groups_hit_regional_ids: String = "",
                     var groups_hit_user_level_ids: String = "",
                     var groups_hit_phone_level_ids: String = "",
                     var groups_hit_os_type_ids: String = "",
                     var groups_hit_black_install_pkg_ids: String = "",
                     var groups_hit_white_install_pkg_ids: String = "",
                     var groups_hit_content_category_ids: String = "",
                     var groups_hit_new_user_ids: String = "",
                     var groups_hit_acc_user_type_ids: String = "",
                     var groups_hit_interest_or_user_signal_ids: String = "",
                     var groups_filtered_by_ad_show_ids: String = "",
                     var groups_filtered_by_ad_click_ids: String = "",
                     var groups_filtered_by_black_user_ids: String = "",
                     var groups_filtered_by_black_sid_ids: String = "",
                     var groups_filtered_by_not_delivery_pr_ids: String = "",
                     var ideas_filtered_by_material_type_ids: String = "",
                     var ideas_filtered_by_interaction_ids: String = "",
                     var ideas_filtered_by_black_class_ids: String = "",
                     var ideas_filtered_by_acc_class_ids: String = "",
                     var ideas_filtered_by_material_level_ids: String = "",
                     var ideas_filtered_by_only_site_ids: String = "",
                     var ideas_filtered_by_filter_goods_ids: String = "",
                     var ideas_filtered_by_chitu_ids: String = "",
                     var ideas_filtered_by_pkg_filter_ids: String = "",
                     var bid_avg_before_filter: Int = 0,
                     var bid_avg_after_filter: Int = 0,
                     var bid_avg_return_as: Int = 0,
                     var ad_num_of_delivery: Int = 0,
                     var media_appsid: String = "",
                     var module_times: String = "",
                     var groups_hit_ad_slot_ids: String = "",
                     var groups_hit_not_allow_delivery_ids: String = "",
                     var ideas_filtered_by_bidrankcut_ids: String = "",
                     var ideas_filtered_by_middle_page_ids: String = "",
                     var involved_ideas_ids: String = "",
                     var matched_ideas_ids: String = "",
                     var rnd_ideas_ids: String = "",
                     var channelid: String = "",
                     var ad_slot_type: String = "",
                     var acc_class: String = "",
                     var black_class: String = "",
                     var slot_width: String = "",
                     var slot_height: String = "",
                     var keyword: String = "",
                     var white_material_level: String = "",
                     var regionals: String = "",
                     var os_type: String = "",
                     var age: String = "",
                     var gender: String = "",
                     var coin: String = "",
                     var interests: String = "",
                     var phone_level: String = "",
                     var adnum: String = "",
                     var new_user: String = "",
                     var network: String = "",
                     var only_site: String = "",
                     var direct_uid: String = "",
                     var material_styles_s: String = "",
                     var interactions_s: String = "",
                     var acc_user_type: String = "",
                     var ideas_filtered_by_qjp_antou_qtt: String = "",
                     var ideas_filtered_by_unknown: String = "",
                     var after_rank_ideas: String = "",
                     var ideas_filtered_by_content: String = "",
                     var embedding_ctr_uniq_ideas: String = "",
                     var embedding_redis_hit: String = "",
                     var ideas_filtered_by_fill_freq_control: String = "",
                     var ideas_filtered_by_show_freq_control: String = ""
  )

}
