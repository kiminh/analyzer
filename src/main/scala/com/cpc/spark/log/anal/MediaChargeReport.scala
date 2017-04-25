package com.cpc.spark.log.anal



/**
  * Created by Roy on 2017/4/25.
  */

case class MediaChargeReport(
                          media_id: Int = 0,
                          adslot_id: Int= 0,
                          idea_id: Int= 0,
                          unit_id: Int= 0,
                          plan_id: Int= 0,
                          request: Int = 0,
                          served_request: Int = 0,
                          impression: Int = 0,
                          click: Int = 0,
                          charged_click: Int = 0,
                          spam_click: Int = 0,
                          cash_cost: Int = 0,
                          coupon_cost: Int = 0,
                          date: String = "",
                          hour: Int = 0
                        ) {

  val key = "%s-%d-%d-%d-%d".format(media_id, adslot_id, plan_id, unit_id, idea_id)

  def sum(mc: MediaChargeReport): MediaChargeReport = {
    copy(
      request = mc.request + request,
      served_request = mc.served_request + served_request,
      impression = mc.impression + impression,
      click = mc.click + click,
      charged_click = mc.charged_click + charged_click,
      spam_click = mc.spam_click + spam_click,
      cash_cost = mc.cash_cost + cash_cost,
      coupon_cost = mc.coupon_cost + coupon_cost
    )
  }
}
