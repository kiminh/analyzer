package com.cpc.spark.ml.recallReport

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar

import bslog.Bslog.NoticeLogBody
import com.cpc.spark.streaming.tools.Encoding

object bs_log_report {
  def main(args: Array[String]): Unit = {
    val beforeNdays = args(0).toInt
    val spark = SparkSession.builder()
      .appName("bs log report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -beforeNdays)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    val stmt: String =
      s"""
        |select trim(split(raw, '\\\\*')[1]) as raw from dl_cpc.cpc_basedata_recall_log
        |where day='$tardate' and length(trim(split(raw, '\\\\*')[1]))>0
      """.stripMargin
    spark.sql(stmt).show
    var excp = 0
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
            exptags=exptags.substring(1, exptags.length()-1)
          )
        } catch {
          case ex: Exception => excp += 1; null
        }

    }.filter(_ != null).toDF("searchid", "involved_group_num", "group_media_num", "group_region_num", "group_l_v_num", "group_os_type_num",
      "group_p_l_num", "group_dislike_num", "group_interest_num", "group_student_num", "group_acc_user_type_num", "group_new_user_num",
      "group_content_category_num", "group_black_install_pkg_num", "group_white_install_pkg_num", "group_show_count_num",
      "group_click_count_num","matched_group_num", "len_groups",
      "involved_idea_num", "matched_idea_num", "rnd_idea_num", "exptags")
    pbData.createOrReplaceTempView("temp_table")
    println(excp)
    val insertIntoTable =
      s"""
         |insert overwrite table dl_cpc.recall_filter_number_report partition (`date`='$tardate')
         |select * from temp_table
      """.stripMargin
    spark.sql(insertIntoTable)
/**
    val pbData = spark.sql(stmt).rdd.map{
      r =>
        val pb = r.getAs[String]("raw")
        val up = NoticeLogBody.parseFrom(Encoding.base64Decoder(pb).toArray).toBuilder
        val searchid = up.getSearchid
        val req_io_time = up.getReqIoTime
        val process_time = up.getProcessTime
        val bs_rank_tag = up.getBsRankTag
        val ctr_version = up.getCtrVersion
        val module_times = up.getModuleTimesList.toString
        val rnd_idea_num = up.getRndIdeaNum
        val matched_idea_num = up.getMatchedIdeaNum
        val involved_idea_num = up.getInvolvedIdeaNum
        val fG_cal_make_time = up.getFGCalMakeTime
        val fG_for_loop_time = up.getFGForLoopTime
        val fG_assigment_time = up.getFGAssigmentTime
        val fG_foor_loop_count = up.getFGFoorLoopCount
        val search_cond = up.getSearchCond
        val group_stats = up.getGroupStats
        val hostname = up.getHostname
        val exptags = up.getExptagsList.toString
        (searchid, req_io_time, process_time, bs_rank_tag, ctr_version, module_times.substring(1, module_times.length()-1),
          rnd_idea_num, matched_idea_num, involved_idea_num, fG_cal_make_time, fG_for_loop_time, fG_assigment_time,
          fG_foor_loop_count, search_cond, group_stats, hostname, exptags.substring(1, exptags.length()-1))
    }.toDF("searchid", "req_io_time", "process_time", "bs_rank_tag", "ctr_version", "module_times",
      "rnd_idea_num", "matched_idea_num", "involved_idea_num", "fG_cal_make_time", "fG_for_loop_time", "fG_assigment_time",
      "fG_foor_loop_count", "search_cond", "group_stats", "hostname", "exptags")
    pbData.show(10)
  */
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
                     var group_content_category_num: Int,
                     var group_black_install_pkg_num:Int=0,
                     var group_white_install_pkg_num:Int=0,
                     var group_show_count_num:Int=0,
                     var group_click_count_num:Int=0,
                     var matched_group_num:Int=0,
                     var len_groups:Int=0,
                     var involved_idea_num:Int=0,
                     var matched_idea_num:Int=0,
                     var rnd_idea_num:Int=0,
                     var exptags: String=""
                   )
}
