package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/7/10 19:45
  */
object KDDBaseData {
    def main(args: Array[String]): Unit = {
        val day = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"KDDBaseData day = $day, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select searchid
               |    ,ideaid
               |    ,insertion_id
               |    ,hostname
               |    ,timestamp
               |    ,timecost
               |    ,network
               |    ,ip
               |    ,isp
               |    ,ua
               |    ,media_type
               |    ,media_appsid
               |    ,channel_id
               |    ,media_app_pkgname
               |    ,media_app_version
               |    ,media_app_versioncode
               |    ,floorbid
               |    ,media_site_url
               |    ,client_requestid
               |    ,client_isvalid
               |    ,client_version
               |    ,client_type
               |    ,browser_type
               |    ,content_id
               |    ,category
               |    ,siteinfo_title
               |    ,siteinfo_conf
               |    ,key_words_list
               |    ,dtu_id
               |    ,adslot_id
               |    ,adslot_type
               |    ,interact_pagenum
               |    ,interact_bookid
               |    ,cpmbid
               |    ,exp_style
               |    ,channel
               |    ,chapter_id
               |    ,country
               |    ,province
               |    ,city
               |    ,city_level
               |    ,geo_lnt
               |    ,geo_lat
               |    ,geo_alt
               |    ,geo_loctime
               |    ,uid
               |    ,brand_title
               |    ,model
               |    ,screen_w
               |    ,screen_h
               |    ,os
               |    ,phone_price
               |    ,phone_level
               |    ,os_version
               |    ,life_time
               |    ,tkid
               |    ,tuid
               |    ,lx_type
               |    ,lx_package
               |    ,sex
               |    ,age
               |    ,coin
               |    ,share_coin
               |    ,qukan_new_user
               |    ,user_create_time
               |    ,qtt_member_id
               |    ,user_province
               |    ,user_city
               |    ,interests
               |    ,isfill
               |    ,interaction
               |    ,bid
               |    ,price
               |    ,ctr
               |    ,cpm
               |    ,adtype
               |    ,adsrc
               |    ,adclass
               |    ,siteid
               |    ,unitid
               |    ,planid
               |    ,material_level
               |    ,ad_title
               |    ,ad_click_url
               |    ,adid_str
               |    ,trigger_type
               |    ,downloaded_app
               |    ,user_cvr_threshold
               |    ,usertype
               |    ,final_cpm
               |    ,userid
               |    ,click_count
               |    ,click_unit_count
               |    ,long_click_count
               |    ,long_req_count
               |    ,user_req_ad_num
               |    ,user_req_num
               |    ,dsp_num
               |    ,embedding_num
               |    ,bs_num
               |    ,exptags
               |    ,cvr_real_bid
               |    ,cvr_threshold
               |    ,exp_ctr
               |    ,exp_cvr
               |    ,raw_ctr
               |    ,raw_cvr
               |    ,boost_cpm
               |    ,boost_win
               |    ,is_new_ad
               |    ,is_content_category
               |    ,rank_discount
               |    ,ctr_model_name
               |    ,cvr_model_name
               |    ,abtest_used_ids
               |    ,is_api_callback
               |    ,vip_first_category
               |    ,vip_price
               |    ,vip_value
               |    ,vip_title_template
               |    ,vip_outer_id
               |    ,dynamic_material_type
               |    ,is_ocpc
               |    ,bid_ocpc
               |    ,exp_feature
               |    ,exp_ids
               |    ,ocpc_log
               |    ,bs_rank_tag
               |    ,as_filtered_num
               |    ,ext_int
               |    ,ext_string
               |    ,ext_float
               |    ,is_auto_coin
               |    ,fill_offset
               |    ,isshow
               |    ,show_timestamp
               |    ,show_network
               |    ,show_ip
               |    ,show_refer
               |    ,show_ua
               |    ,video_show_time
               |    ,charge_type
               |    ,touch_x
               |    ,touch_y
               |    ,isclick
               |    ,slot_width
               |    ,slot_height
               |    ,click_timestamp
               |    ,click_network
               |    ,click_ip
               |    ,antispam_score
               |    ,antispam_rules
               |    ,spam_click
               |    ,dsp_adslot_id
               |    ,dsp_media_id
               |    ,dtu_restriction
               |    ,exp_cpm
               |    ,conversion_goal
               |    ,conversion_from
               |    ,battery_percent
               |    ,page_scroll_timeval
               |    ,pois
               |    ,abexp_ids
               |    ,bid_discounted_by_ad_slot
               |    ,charge_state
               |    ,dsp_cpm
               |    ,second_src
               |    ,second_cpm
               |    ,discount
               |    ,cpc_price
               |    ,scpc_price
               |    ,dsp_pk_weight
               |    ,dsp_bid
               |    ,bidfloor
               |    ,delivery_type
               |    ,dsp_mincpm
               |    ,dsp_randv
               |    ,final_dsp_mincpm
               |    ,dsp_floorcpm
               |    ,charge_method
               |    ,req_dsp_num
               |    ,resp_dsp_num
               |    ,pk_dsp_num
               |    ,activation_time
               |    ,new_user_days
               |    ,ocpm_ratio
               |    ,bsctr
               |    ,bscvr
               |    ,bsrawctr
               |    ,bsrawcvr
               |    ,opt_material_id
               |    ,sub_cate
               |    ,coin_add_flag
               |    ,price_mark_pos
               |    ,expctr_mark_pos
               |    ,expcpm_filter_thr
               |    ,price_mark_pos_first
               |    ,expctr_mark_pos_first
               |    ,expcpm_thr_filter_num
               |    ,bs_rnd_ads
               |    ,after_mt_filter_ads
               |    ,after_rank_ads
               |    ,bs_rnd_ads_avg_bid
               |    ,after_mt_filter_ads_avg_bid
               |    ,after_rank_ads_avg_bid
               |    ,after_rank_ads_avg_expcpm
               |    ,bottoming_union
               |    ,opt_material_adid
               |    ,opted_material_title
               |    ,opted_material_description
               |    ,opted_material_adtype
               |    ,opted_material_imageurl
               |    ,ocpc_step
               |    ,previous_id
               |    ,traffic_sources
               |    ,agent_id
               |    ,ocpc_status
               |    ,adbug_video_id
               |    ,wx_name_site
               |    ,material_selective_flag
               |    ,virtual_opt_type
               |    ,virtual_opt_step
               |    ,inspire_video_type1
               |    ,day
               |    ,hour
               |from dl_cpc.cpc_basedata_union_events
               |where day = '$day' and hour = '$hour'
               |and media_appsid in ('80002819')
             """.stripMargin

        val baseData = spark.sql(sql)

        baseData.repartition(200)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_kdd_basedata_union_events")
    }
}
