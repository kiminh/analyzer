package com.cpc.spark.ocpcV3.ocpc.filter

import com.alibaba.fastjson.JSONObject

class OcpcLightControlUnit(id: String) {
  var identifier: String = id
  var cpaSuggest1: Double = 0.0
  var cpaSuggest2: Double = 0.0
  var cpaSuggest3: Double = 0.0
  var isRecommend1: Int = 0
  var isRecommend2: Int = 0
  var isRecommend3: Int = 0

  def toJsonString = {
    val json = new JSONObject()
    if (isRecommend1 == 1) {
      json.put("download_cpa", cpaSuggest1)
    }
    if (isRecommend2 == 2) {
      json.put("appact_cpa", cpaSuggest2)
    }
    if (isRecommend3 == 3) {
      json.put("formsubmit_cpa", cpaSuggest3)
    }
    json.toString
  }

  def print = {
    println(s"download_cpa: $cpaSuggest1, is_recommend: $isRecommend1")
    println(s"appact_cpa: $cpaSuggest2, is_recommend: $isRecommend2")
    println(s"appact_cpa: $cpaSuggest3, is_recommend: $isRecommend3")
  }

  def setCPA(cpaValue: Double, cvGoal: Int) = {
    if (cvGoal == 1) {
      cpaSuggest1 = cpaValue
    } else if (cvGoal == 2) {
      cpaSuggest2 = cpaValue
    } else {
      cpaSuggest3 = cpaValue
    }
  }

  def setRecommendation(isRecommend: Int, cvGoal: Int) = {
    if (cvGoal == 1) {
      isRecommend1 = isRecommend
    } else if (cvGoal == 2) {
      isRecommend2 = isRecommend
    } else {
      isRecommend3 = isRecommend
    }
  }
}

object OcpcLightControlUnit {

}
