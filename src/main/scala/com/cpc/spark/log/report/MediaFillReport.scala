package com.cpc.spark.log.report

/**
  * Created by Roy on 2017/4/26.
  */
case class MediaFillReport (
                        media_id: Int = 0,
                        adslot_id: Int = 0,
                        adslot_type: Int = 0,
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

  val key = "%d-%d".format(media_id, adslot_id)

  def sum(r: MediaFillReport): MediaFillReport = {
    copy(
      request = r.request + request,
      served_request = r.served_request + served_request,
      impression = r.impression + impression,
      click = r.click + click,
      charged_click = r.charged_click + charged_click,
      spam_click = r.spam_click + spam_click,
      cash_cost = r.cash_cost + cash_cost,
      coupon_cost = r.coupon_cost + coupon_cost
    )
  }
}

