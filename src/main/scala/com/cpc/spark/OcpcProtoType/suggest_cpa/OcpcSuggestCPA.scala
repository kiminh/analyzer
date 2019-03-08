package com.cpc.spark.OcpcProtoType.suggest_cpa

import org.apache.spark.sql.SparkSession


object OcpcSuggestCPA {
  def main(args: Array[String]): Unit = {
    /*
    新版推荐cpa程序：
    unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, cal_bid, auc, kvalue, industry, is_recommend, ocpc_flag, usertype, pcoc1, pcoc2

    主要源表：dl_cpc.ocpc_base_unionlog, dl_cpc.ocpc_label_cvr_hourly

    数据构成分为以下部分:
    1. 基础数据部分：unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
    2. ocpc部分：kvalue
    3. 模型部分：auc
    4. 计算部分

     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour")
      .enableHiveSupport().getOrCreate()


  }
}
