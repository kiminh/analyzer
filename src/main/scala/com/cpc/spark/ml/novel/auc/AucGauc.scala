package com.cpc.spark.ml.novel.auc

/**
  * @author Jinbao
  * @date 2018/12/22 17:45
  */
object AucGauc {
    case class AucGauc(var auc:Double = 0,
                       var gauc:Double = 0,
                       var adslot_type:Int = 0,
                       var model:String = "",
                       var date:String = "",
                       var hour:String = "")
}
