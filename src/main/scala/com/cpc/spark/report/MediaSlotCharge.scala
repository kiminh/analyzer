package com.cpc.spark.report

case class MediaSlotCharge(
                            media_id: Int = 0,
                            media_type: Int = 0,
                            media_name: String = "",
                            adslot_id: String = "",
                            adslot_type: Int = 0,
                            idea_id: Int = 0,
                            unit_id: Int = 0,
                            plan_id: Int = 0,
                            user_id: Int = 0,
                            uid: String = "",
                            uid_type: String = "",

                            adclass: Int = 0,
                            adtype: Int = 0,
                            dsp: Int = 0,
                            charge_type: Int = 0,
                            ctr_model_name: String = "",
                            cvr_model_name: String = "",

                            request: Int = 0,
                            fill: Int = 0,
                            impression: Int = 0,
                            click: Int = 0,
                            charged_click: Int = 0,
                            spam_click: Int = 0,
                            cost: Int = 0,

                            ctr: Double = 0.0,
                            cvr: Double = 0.0,
                            cpm: Double = 0.0,
                            acp: Double = 0.0,
                            arpu: Double = 0.0,

                            date: String = ""
                          ) {
  val key = "%d-%d-%d-%s-%d-%d-%s-%s".format(media_id, adslot_id, idea_id, uid, dsp, charge_type, ctr_model_name, cvr_model_name)
   
  def sum(other: MediaSlotCharge): MediaSlotCharge = {
    copy(
      request = other.request + request,
      fill = other.fill + fill,
      impression = other.impression + impression,
      click = other.click + click,
      charged_click = other.charged_click + charged_click,
      spam_click = other.spam_click + spam_click,
      cost = other.cost + cost
    )
  }

}
