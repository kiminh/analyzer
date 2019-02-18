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

    val group = Group(
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
      group.copy(ideaid = ideaid)
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

//case class Group(
//                  unitid: Int = 0,
//                  planid: Int = 0,
//                  userid: Int = 0,
//                  price: Int = 0, // 单位为分
//                  chargetype: Int = 0,
//                  regionals: String = "", //限制地域,不限制为空
//                  os: Int = 0, // 限制os
//                  ideaid: Int = 0, // 单元下面的创意id
//                  adslottype: Int = 0, //
//                  timepub: String = "", // 从0-6表示周一，周二...周日 从第0位表示0-1点，第1位表示1-2点
//                  blacksid: String = "", // 黑名单appsid
//                  whitesid: String = "", // 白名单appsid
//                  ages: String = "", // 年龄定向
//                  gender: Int = 0, // 0 全部  1男 2女
//                  freq: Int = 0, // 频次控制，0表示不控制
//                  userlevel: Int = 0, //用户积分级别. 0默认全选 1第一档用户，积分在0-10分。 2第二档用户，积分在0-1000分。 3第三档用户，积分在0-10000分。4全选
//                  userType: Int = 0, //' 黑五类标记 0 非黑五类 1 黑五类'
//                  interests: String = "", //兴趣标签
//                  media_class: String = "", //定向媒体分类
//                  phonelevel: String = "", // 设备等级，多选 1高端 2中高 3中低 4低端
//                  cvr_threshold: Int = 0, //cvr 截断阈值
//                  balance: Int = 0, //min(计划所剩余额,账户余额)
//                  new_user: Int = 0, // 0全选  2老用户 1新用户
//                  user_weight: Int = 0, //广告主权值 0未设置 非0为设置了阈值，单位%
//                  network: String = "", // 0:unknown 1:wifi  2:2G   3:3G  4:4G
//                  black_install_pkg: String = "", // 过滤的安装包信息
//                  dislikes: String = "", // 不感兴趣标签
//                  click_freq: Int = 0, //点击频率上限
//                  white_install_pkg: String = "", // 安装包定向
//                  student: Int = 0, //职业定向，学生 224，非学生 225 ，全部 0
//                  is_ocpc: Int = 0, // 是否启用ocpc
//                  content_category: String = "",
//                  ocpc_price: Int = 0, // 单位为分
//                  ocpc_bid_update_time: Int = 0, // ocpc出价最后更新时间，秒级时间戳
//                  conversion_goal: Int = 0, // 转化目标，1.安装，2.激活，3.表单，
//                  target_uids: String = "", //单元定向的uid
//                  delivery_type: Int = 0, //配送方式 0：无配送 1：趣头条配送外媒 2: 趣头条配送给小说
//                  adslot_weight: String = "",
//                  target_adslot_ids: String = "" //定向广告位id
//                )
