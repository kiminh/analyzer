package com.cpc.spark.ml.ctrmodel.v1

/**
  * Created by roydong on 18/12/2017.
  */
case class Features (
                      searchid: String = "",
                      timestamp: Int = 0,
                      network: Int = 0,
                      ip: String = "",
                      media_type: Int = 0,
                      media_appsid: Int = 0,
                      adslotid: Int = 0,
                      adslot_type: Int = 0,
                      adnum: Int = 0,
                      adtype: Int = 0,
                      interaction: Int = 0,
                      bid: Int = 0,
                      floorbid: Float = 0,
                      price: Int = 0,
                      ideaid: Int = 0,
                      unitid: Int = 0,
                      planid: Int = 0,
                      country: Int = 0,
                      province: Int = 0,
                      city: Int = 0,
                      isp: Int = 0,
                      model: String = "",
                      uid: String = "",
                      ua: String = "",
                      os: Int = 0,
                      sex: Int = 0,
                      age: Int = 0,
                      coin: Int = 0,
                      isshow: Int = 0,
                      isclick: Int = 0,
                      userid: Int = 0
                    ) {

}
