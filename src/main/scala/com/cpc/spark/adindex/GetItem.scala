package com.cpc.spark.adindex

import idxinterface.Idx.{GroupItem, IdeaItem}

object GetItem {
  def getIdea(ideaItem: IdeaItem): Idea = {

    val idea = Idea(
      ideaid = ideaItem.getIdeaid,
      mtype = ideaItem.getMtypeValue,
      width = ideaItem.getWidth,
      height = ideaItem.getHeight,
      interaction = ideaItem.getInteractionValue,
      `class` = ideaItem.getClass_,
      material_level = ideaItem.getMaterialLevelValue,
      siteid = ideaItem.getSiteid,
      white_user_ad_corner = ideaItem.getWhiteUserAdCorner
    )
    idea
  }

  def getGroup(groupItem: GroupItem): Seq[Group] = {
    var groups = Seq[Group]()

    var group = Group(
      unitid = groupItem.getGroupid,
      planid = groupItem.getPlanid,
      userid = groupItem.getUserid,
      price = groupItem.getPrice,
      charge_type = groupItem.getChagetypeValue,
      regionals = groupItem.getRegionalsList.toArray.mkString(","),
      os = groupItem.getOsValue,
      adslot_type = groupItem.getAdslottypeValue,
      timepub = groupItem.getTimepubList.toArray.mkString(","),
      blacksid = groupItem.getBlacksidList.toArray.mkString(","),
      whitesid = groupItem.getWhitesidList.toArray.mkString(","),
      ages = groupItem.getAgesList.toArray.mkString(","),
      gender = groupItem.getGender,

      freq = groupItem.getFreq,
      userlevel = groupItem.getUserlevel,
      usertype = groupItem.getUserType,
      interests = groupItem.getInterestsList.toArray.mkString(","),
      media_class = groupItem.getMediaClassList.toArray.mkString(","),

      phonelevel = groupItem.getPhonelevelList.toArray.mkString(","),
      cvr_threshold = groupItem.getChagetypeValue,
      balance = groupItem.getBalance,
      new_user = groupItem.getNewUser,
      user_weight = groupItem.getUserWeight,
      network = groupItem.getNetworkList.toArray.mkString(","),
      black_install_pkg = groupItem.getBlackInstallPkgList.toArray.mkString(","),
      dislikes = groupItem.getDislikesList.toArray.mkString(","),

      click_freq = groupItem.getClickFreq,
      white_install_pkg = groupItem.getWhiteInstallPkgList.toArray.mkString(","),
      student = groupItem.getStudent,
      isocpc = groupItem.getIsOcpc,
      content_category = groupItem.getContentCategoryList.toArray.mkString(","),
      target_uids = groupItem.getTargetUidsList.toArray.mkString(","),
      delivery_type = groupItem.getDeliveryType,
      adslot_weight = groupItem.getAdslotWeight,
      target_adslot_ids = groupItem.getTargetAdslotIdsList.toArray.mkString(",")
    )
    val ideaidsCount = groupItem.getIdeaidsCount
    for (i <- 0 until ideaidsCount) {
      val ideaid = groupItem.getIdeaids(i)
      group = group.copy(ideaid = ideaid)
      groups :+= group
    }
    groups

  }

}

case class Idea(
                 var ideaid: Int = 0,
                 var mtype: Int = 0,
                 var width: Int = 0,
                 var height: Int = 0,
                 var interaction: Int = 0,
                 var `class`: Int = 0,
                 var material_level: Int = 7, //素材级别
                 var siteid: Int = 0, //建站id，0：非建站
                 var white_user_ad_corner: Int = 0 //白名单用户无广告角标，0：关闭，1：开启
               )


case class Group(
                  var timestamp: Int = 0,
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
                  var whitesid: String = "",
                  var ages: String = "",
                  var gender: Int = 0,
                  var freq: Int = 0,
                  var userlevel: Int = 0,
                  var usertype: Int = 0,
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


