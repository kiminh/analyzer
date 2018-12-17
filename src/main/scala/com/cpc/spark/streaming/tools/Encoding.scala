package com.cpc.spark.streaming.tools

import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.binary.Base64
import scala.collection.mutable.HashMap

object Encoding {
  val CODE_TRANS_MAP = new HashMap[Int,HashMap[Char,Char]]
  val codes = Array(
    "0Ge5Q6wnuBML3Wg-8s7kAjOr2xohRHif1CczEXDKq_VaUPJdYpy9TSNvIFmbZ4tl",
    "1RPspa70TiZum_6tKNIFOVJ4o3gBGkdLlvhCnQHWDxEA5cq8fYzj2XU9MwrS-ybe",
    "2txfHr4BFaqSOTVn_CuYozeygvbcL1RNKs8MkWUPG0JpmXd5l6hDIAjw7i9E-Q3Z",
    "3c5gtSlVFoEXsehLaMJwI0Rv6U7xmWuzCYABP_TDp-QfdKniNrq2Ob419jHkGZy8",
    "456tTGIcm3EK7_RQpBA2uezWXjqnHPsi9bV-Zx8vSgkwaMlCoJ1DyfLFr0UYNhdO",
    "5zDkOUe_q7a2TN-Y3fvrnsCHZxhBIdcESPj8GuWXLgib4QwJo6yVtMF0Kp1mA9Rl",
    "6bgkOJjx8mPI9Y_hWANypSoFtnDadivQ31crfR0uC-K2qzE4UslMXGBTL5Vw7HeZ",
    "7zXOg9UZp0sa2vWGJP3TCBYR-6eIfHVnmdKwMrbuSlo8LFNthAkqcDy4QxE5ji1_",
    "8iMsWOQuApUEhoITJfjbYdLqrcF3m17XwNa_B4lVSC-Deny0KPtG6z2k5gx9vZHR",
    "9gdocC-fpejPiYnHF7l0XrWUAaS3OI_Nm2uDkyZbVGRJq5vt1QzhM6xTLw4BsK8E",
    "aJEr4IWdD3Zxw8ClmNzGPHFL9usbKTvXU-2eAkiV16ScBOy7pqgQnR0_j5MYftho",
    "bZqml65BeDJETLrNpMAC-tIOhVXsnf1w9_Szv7gP2xaWKR0cQ4F3Uuo8YHGdjiky",
    "cIJjl64KtC_doMuUeEq0fv831F2kDnNshAbzVaZTprYHB-mWXwygQ7RGxi9PL5SO",
    "dZgpqwv-321tRL5ITib0X8Uy64kKjoQuOsrGENxMBfY9WDea7HcmJnAC_PzSlFVh",
    "eUOghPNMEVoxkiG7YQL0Zmjc_IWsdl2FtRufySH4DvATBwbXCK9z5-par8J61nq3",
    "fP1pUQ3_moNy-7FXHAGT8uKL4wszIVMjaRdhi25E9O0kWntrBvScl6DZgqbxYJCe",
    "g43jISvAuK6UND0qYma2QXxRPyBib1l_zCtke5VpsEJOMf7LdZHGcFnw-hW9o8Tr",
    "humQGil2xCkHY391US7DvZVEbe_PR4NXWqKgrFfJApd60Bt8yno5IMawsj-OLzcT",
    "i2MxZ56X0LWSQGzpONHwPje_lA1nftYIvRTr-9UJs4DkydcgoVqBCu3bE7Kh8mFa",
    "jk9A_YBn6aqWt1yfm38SLJ7z-c42erUEHxhb5dROGgNoIusvKCDXQMTp0lZiPwVF",
    "kCBHjyTZmhSEwfY8JPdX9MpoKWuL61FQDtOnzab-s3INrc4qi0e7lA_gxUV5R2Gv",
    "lToWfMpEHAcir_JLRdBDSkaU5hPnmOCqVZ2KXNzI4uwGx01Ys379egtQ8yF-vbj6",
    "m36NTQ82RhD4E-tMfxjs0CPoLbirI_WBZHgnFcypezvkadXSGwO95lAKYJ7Uu1qV",
    "niOyHQ6oFbWzeM1qmu4tV9grTkldhGj20NELD3pRU8ZSAa7-wxKJsYcPBI5_vCXf",
    "odnysI_h-FGKmJQ2NvUwgVLcaOHCbSDAqlxp85Zurjzi4Mft793RYPe6TE01XkWB",
    "p0tGYHmhNTfdFkMQbUc1axCq4y5viXD89suS3BAIPL67VKrWolOgzwRZ_eE2jJ-n",
    "qrw0jSf_3pxth-LZnPoBITkGAvl7gQKdF69CVyWaD5izE4mN8ecMRHYOu2UJs1Xb",
    "rjUafJvNocE3D8TiQYVAHLp7WbSnzemqugBy5lIFk10_2CdM-Gh69Ksx4XRZtOwP",
    "sPWiyNlL3rGBI8gtfxpDbAoUe_5uhzMjZd09cTCamEwYJF6k14QvnVK7Oq-R2HXS",
    "tHir6_aKGnqf1RQABh3IzSwYysp4kWdUPCce2l7ZV8XJO9MmLbg50NT-vFuEoxjD",
    "uvfDMaj4s7hUr0GBSxWqyV6z-gnLH9QCFoJZwPK8_1lXYON2RpATId5bimEtce3k",
    "vy01f3dLjbn4RQuSpiEhKV_XaMB6l2cgxFI-8oUWPAY95wtmZkJNzHOCqrTe7sGD",
    "wDmOP9vQr_tyguqxobLHzjMC50-IAJiB4EZf1nh36sl8VWTXYGF2d7UaeSkcKpRN",
    "xyqhnozlE_1cV8mukNLTtHfW6r5gw2Se09A4BC3OvJdIXFpsRDiM7YbPGUajKQZ-",
    "y6D3Z4lYsGaxOd0nE9irmRthB7ILkPW-NbCjQpSUe12gcFK8uvofzXq5_TAHMJVw",
    "zH-ExpCWvMa4iPKkDdeTwq3_moAIQFXSUYGgBbL07NftO2r8nJ6lsRyu5cZhV91j",
    "ApKPs5S8LviIeqzyGMEmkrUhJdRgx-Q12VZFlTt9b7Y3NoBc_uOaD0CfWXHn4jw6",
    "BTUcJEezrnZg1YKoD3Iy_aGNOPCQtiljm90LvVWXkx48p5AHhf-dqSu67MsFwb2R",
    "Cfxmu205MWqpTElyPLQhODzjG36JwdsYVINZvSr471tAk_nea-R89oHbFKBicgXU",
    "De9UpodQBKy12HC_f6xRkYOgXv-7PNEWbuLA0qaGTScs54inrwJmZzjMF3IlVh8t",
    "E1RkI23NpneclxgmPyZ685Twv4aGrfCQsib-0Md9JhDO7F_VUSHqKYouAWjtBXLz",
    "Fu1SjpgwfAm5hU3DGdYnEsIX-kl_oaN62KWctPb098CezHZqOR4vVB7yQLixTrJM",
    "Gv7QT10t6edVH59gcLWOEAblzRkPyFCYNXf4I82iMmwUB_3Z-axKSspnojuJhqrD",
    "H-lduv6Xih9fQmF3O4JxMY7pcIjTnoatSs0CZBe15U_RyrLgPN8EwkAKGbDzW2Vq",
    "I9h-UfpFRlVGb_O10Zai3sq7dALjyDte2SzvMCBcYuxK5H8JwNXTPkEWr6gQno4m",
    "J-fYWMd6jFomtR5SP9lcqEn4aHAVx0ONILrCsyQi_pwTUhg73BvKDz2u1ZkXe8bG",
    "KboJvRSdBLe_r9EDz8HO-Z5GI0PW7cQuNns63gAhtYilTkqwyCF1xUVaM2m4Xfpj",
    "LmPDzctkaYWfSZn4yRCIBl253dw8sb9VxJEAXU6erN_Ou0GvMQg-Ko1iT7FhqpjH",
    "M1DFQXKTH-fJtUwBpngi04odZV3eYO9GuW86NrLh7xEslRP2z_aCkIjAcym5qSbv",
    "NuzOPiFag3dWUewMmyTEB5kYhcxDL2fbr_V8nRC9HGI0ZQ67KXo-lAqvJSs41jtp",
    "OXE5d2WJIKelZS0wj3tN6xvhP1QMrF8-nDka7HTyLi_YgVCGAzubomUf49BRcqps",
    "PIoQ0Vd-DgF9aYLHyGtvBcX61x8mJ_AKMU5bR3sruwTlfh42knSejNiWEqp7OZCz",
    "QrfzshiqxbpXZGuPV4_HL62on8YwSWITAc-eyO3djvKgNa5mE7tD10BFCMkJUl9R",
    "R3kYn_jgNAOJCdSwHol84aQbD7U-IhBciV20EuKM9tfrFxZ6XqzvmpLW1GPT5sey",
    "SLfn20Wt7oU1zMqrYkJcFZ9BmaOueV3sPTKQ5N-xbiI8DC6yp4gAvXGHlwhdRE_j",
    "T3JinH-K2NPsx4MuacYrXvdmWZjeOAQfp9C8tDz075qE1yVUBRb_L6whSGFoklIg",
    "UCvz-5K34p9OBSL8tkImg21wGYFfl6NaciZnDx7sRW0b_eyoAEQPrXqMjhHTduJV",
    "V4miLp8wCY0QGx7oB6_FvjWJeRs2dZK5XDNOAkhUbuMcHanT3zEIlgyr9tf-1SPq",
    "Wxd0EnjzS6DXcIR1qUoJMmF_fGa8uHs5h9A3VkbTiCZvNplLryKwQY4BOPg7et-2",
    "XYc4wekTRE8ZoxQ9dtiyO-3aAfuIjVsCFh_7Jp20rqm5SHWL1lBbnUNzDKvGPMg6",
    "YHS9KRLhdqbMtaeX6wG4QNp21sBFrIV_y7DlCzOx80UiumWAnT5-PkgocJEf3jZv",
    "Z_FmazrYTKXHW-4sSbk86D5VPyfN13gBu9deJE7q2ontjQOGhxwiCclUAMvLp0IR",
    "_rJb0Up6R2hNDWFAH3LswnYd7zPQo1M8KxyIjBugESqTGZtmleifcVkCX9v5-aO4",
    "-6mTYvy7KRxCr2elEiWbL5qMoc9G1_3untsdpjwDAzFVhZSgBN0aIOU4QkH8fJXP",
    //new
    "kuRvQ593axq28zIjnwg-E7mXUVpO_Hoi0DAL6fbWTSJMCtKesB4chYFryN1PdZlG",
    "mzlHiPxgCnkTM9frNha4_SKY-5Zc8G0RJE6VL7AX3WDOoQUw1buqB2vjtFpesdIy",
    "xhQwMPBmEiHXF_dK-UvRI4rJnCsD7ZutlaogON160q3eTyVLzk5bcWpj2fA98GSY",
    "dzlmnhQk75oBP6fr03LGgO4TMDW2aUNvsKxXHwpi9bIeCE18RVJuqcAtYZjF_Sy-",
    "6NaZkTgQwfO8DFEXtbCRiGPU_0m-KLYeu97qrvjnJ5do2s4WxBIhSyAV3pMlHcz1",
    "4w-SgkW0jzLU5bfN6mRHraKyBxdiXo3FZhOvtnAG1lTP8EpeV9DCcIq7QuYsJ2M_",
    "KxZ7sMLW9f_EFYU-4nCQuHw2hIBdJT5caeSljqyr8gzp6GD3RvOmAVX1Ni0Ptbok",
    "efXmrn9SJEp0dBZilgbC_uhkyDxjztYcNI6HFaoA8PU2-QM47VT15q3RGLsWwKvO",
    "rdzUm12SXJsINFxqAPnOQY5Kp6fHT_Dk3MoWBRbiL8CwjZh9Elyevuc4Ga-0Vtg7",
    "3MGJgTOVjAdhnP7i2EkKyZxoaeR_UqmW68clBbfHDQSNzYu9Ir-wCFs1Lptv45X0",
    "azwg9IyWLO0vFJGbljQBnT4Nerofm5dDp3PxY-7qM2RcAUS_6XhE8utiCKsZ1kVH",
    "McpPhDaWlfF6dbNz284iK3sQq7-TUCHLoYVtnJr9ZGwE51kRxeu0vBXjISgmAy_O",
    "6IBrJc4Xx7ZtgjpGe8TUYhOwQPS_3DzV2sAR1NMHkE5aWn9FKLvuC0yiqbl-fmod",
    "6-xWNcR_TbsgerfFhVMAIBPnwYimEtulyJo8Hk1CXZ37q2OLv4U0SD9GQdKp5azj",
    "Y7zlAZnsMKq2eSdcwv-96LakVfuXxTPJRUN0Q1EtBCyojiO4WDGbp_8Hrmh35FgI",
    "M-r8qELQATm23wUGx79gHhejNiaWocXDK16PunCsOtRfk4IBbpS5_VJyY0lZFvdz",
    "I2JB1zvGncjNerWCq7wi0tHmhyfSOdQl9a8RpUxsEoDkMZbL4-KTAuYXVP53_F6g",
    "Mg1n6R9qCctFXKaPrZEvAxDp-jJfT_7zYIh0yGeOoluWS8BHk54VmiN2Us3LwbdQ",
    "canrZFSMVkN2_0svH-1eqmuUECQ6gBb734h8dD5KtPlJpiIoxTWY9fjGAzyRXOwL",
    "wck5Q8M7ndZKyfWANoPVLtXsz3hCOmjHGDBbSv_pquar6YRI-Ee29T41J0glFxUi",
    "dRtaT9gBNOAuWH4rxnDzShG-cVvjCsl05Ji8UYkMX3f7q6b2pFyweZL_oEImKQP1",
    "_yfLHoBRCXSdh2-I5euUqa7QkT6pmwYGPvl4s9KzZcjWrMV38AxJDNFbg01tiEOn",
    "v96jSsDR87qoEVbgar1Ni0TzfyPZ3wdQI-hM2Xt_xuGOpWnAFUm5ceLYklHKC4JB",
    "FlunTe_dJ21ISo9bUAQ6D4vLjtsqKO8W7NRcfMVP3XCZ50ahHgB-EpyirzwYkGxm",
    "qJCrNxuaysmQfSVYI9eLcdA0zF2owXkMTvZ1h3PHBt-R5GW8liEb_Oj64pUn7gDK",
    "zrjDy6QoTWfa4CvL0pSX5siU9Y8PEAcIhHwb-kNRVOuqtBK2eJdG3lZ_mn7xgFM1",
    "2S4e9FJzcx3nUdfEI_ptqlsyGiLkDmNO0-7CT8rBVA6oZRguK5Yh1aQjHXPvMwWb",
    "1KehYzPaTRxS708fulI9wq-sivWCckoULEZ6DtdAmbMJp4_G3X5HVBygnNOF2jrQ",
    "Ldz9s6yRthIev2TS8YM3OGFlDcwKBNVoqi_xub17UQmA-kWZPC50EnfJp4ajHXrg",
    "ap0KNzWTIFYunMv61Hs5QgflDZG34kr8JjtAm2Sid7VhREX9coyeUCqxwB_LOP-b",
    "Sh45Y8ntusD3cgK-CXd9IqLBUeljR0kMpWbiHFJfywT1AoEOaPm6Z7zxrVGN2_vQ",
    "jd5MwQplTtcfqA8ENC0kahJrX7voxmLW-12I69bR4iKGVyY3gFUnuHesDPZOSz_B",
    "k5l4g6qjCHEY9AitTho7cuQwysK0m2RDMenIxz3GVFb-XJB_WUfdNavLSZ1rO8pP",
    "ViAvW1cbEeB_auZ4OG9m-Y7o8qlSMjR6D3UrnsCL2KHyhtQFXp0Px5TJgfIwkNdz",
    "2uvtyoJEsLOq7SgYaiwceI1nQj5X8bpPhMfNl_WVZ3kC-90AKGU4FRH6rzDxmdBT",
    "4Gh1zBvMIliKpHFk0duLXgPS7nQwOE3AxT6asjbWRrotYfJqyCZDV-_5m9N2U8ec",
    "60q5GbaFupfKQAVEcjI9BOh2TndYC1o3W-HDk_SRyz8wLmlZ7Xv4ieNrgUMsxtJP",
    "kPvWCBdUpHw_F32u-l05rJgNnfoZTtV6GMq9SELKs7OzYIame8Rh1cDy4xbjiAQX",
    "V2-JyA3sZqpdzRT9Ymf1UxGgWLF7u5vIaN_b4CnB0Kk6PH8DeMoSlwtEOXihrQjc",
    "jlw_PxOqC7gdsnRBeYk8KImTHZzXaN3vVQyAMhEGtSWp50FUcJ6-41oL2Dur9bfi",
    "57JHr_QXovDE4GkKBxgPsF6eAzh8ujtM19alT2OpnVWYZ3mUbqL-dwCf0NIiySRc",
    "Ik9Zhj0N1fV5xAdPBnX4g-UrwzCqJpivDOlGoK6Fu28LW7_ctQybSRYMaT3meHEs",
    "9I6-PLYxE2VJsdO3wKqQ8cm5kunURXlbgevZMNr0aiA_4o7zTyGtHfFCBhp1DSWj",
    "OawEMNejRlYQ16XSuB5Zkq_-dJgGW0fKyLpt9rHPnVU7bhTDFc3oA8szxI24iCmv",
    "Nn0-kuTUIXhZg_bFVH4Ll7OofwKY69zRmMSPEdiJa8rpAty2Q31ceGjsDx5qvBCW",
    "LwhGjy_fgvHUb5CVa8Y1ikmZOzel6x9MurTNnXJcWo302-RdptSqADBEQK7s4IFP",
    "08mU5oONsWzqRZKLrufvJY-a6pB7jgMQFkce3AhSVw_T24CEldtnHbPxI1XDy9Gi",
    "NYQfEMw1m7bcnqz4aUSP-rvHel6kBIsXCJ3K_y98iZVtLFjxgW5dOoAhpu0T2RGD",
    "JRrBd5LtKTNnaD-bs6UYC2O1fM3yoXVcjG8Fzukw7vpglh9qiWeI0PSmAE4HxZQ_",
    "naxtdmz5jvCRZqDOHy1rXBJ3MsELuwAV04kTli9Q-7pg2FbcPfNeY6hGU_KSIWo8",
    "0bGVFPs_Wq5nTAXDmlCv8L4tf1NQH67RarzUOk9u2o3hjJg-IiSYcKwdpeExyBMZ",
    "qIsQu7hGD9NKCMLky80EbprftVBelFg2SaT_ncvUO4jY56A1owHZPXxRzJi3mdW-",
    "EHcXQGzPNZm7MJvx9bptTwBO6u3fqUAS_WK2s4Lknrl8YRDd51yg-oVCahieF0jI",
    "FDw4W98r6eIb5MYUKyHLGgmZxfQlqVPSu0XnoA1di3jvhakEO_TJRpNc7sCt-Bz2",
    "wa9TVJ2lLbix_yzKY8764QjvorpFPhGg3cNB1s-ndqOC0MuHtZEefRSAUkXWDmI5",
    "jyvrxEbs6D7gRpFUL8-PSI2QVMuz0H951KZB3whakcnWGm_dqAoOTlXJ4fNCYtie",
    "QFTZUWBLfw6eMlXV10NkS7YKm-x_a9iG5Atcy8DOCPbj2rnvu4oI3ghEHsRdzJqp",
    "GBMXcAz1k3Qy7RO5qfmTgZFdauH4xSK6C2svEwI098iYpNVotbLljJU_reP-WnDh",
    "4yDgI2rFZTeJNWctoCiSmBf7_z1UaK3d0xPkHnps9jEQqlLVXOv5YwbG8RMuA-6h",
    "ErxIpkh9BUJT5tnd-wCHAFDXY2S8iG_lVo7mfWz6NvqRKLycsZb1gOe43Q0aMjPu",
    "u2_qnxrvClRwWiZpSME3KsTfaNI40Aobty-XV6UBjdLh1OekFJcPHmgz85QYD9G7",
    "UEW7QzjR6r9BfNDYhKPId_vc-8gFaoGxy0AknwsSp3HlCibJ5tZeLTVMu1O4Xqm2",
    "pmT5CbaMYq2sdZDnVUofcul0NwvWKRyih-X6Btg87rSGQ_IF9kAx4jOe1JPzE3LH",
    "6C7Xt4EQ9Z2Ye1qGU5ObgPKn_LrWzD3ohdvxpSMsyiBwIH0lAkf-uNjaR8JcTmFV"
  )

  val standard_base64_code
  = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

  val code_200_for_install_package = "0Ge5Q6wnuBML3Wg-8s7kAjOr2xohRHif1CczEXDKq_VaUPJdYpy9TSNvIFmbZ4tl"

  def base64Decoder(src: String): Seq[Byte] = {
    return Base64.decodeBase64(src)
  }

  def base64Decoder(src: String, code_id: Int): Seq[Byte] = {
    var src1_tmp = src
    if (code_id >= 128 && code_id < 0) {
      throw new IllegalStateException("base64 codeId error codeId="+code_id);
    }
    if (src.length() % 4 != 0) {
      src1_tmp = src + "=" * (4 - src.length() % 4)
    }
    val based = translate(src1_tmp,standard_base64_code,code_id)
    Base64.decodeBase64(based)
  }

  def translate(src:String,std_code:String,code_id:Int):String = {
    if(CODE_TRANS_MAP.contains(code_id)){
      val map = CODE_TRANS_MAP(code_id)
      return src.map{ x => map(x) }
    }else{
      val map = new HashMap[Char,Char]
      var new_code = standard_base64_code
      if(code_id == 200){
        new_code = code_200_for_install_package
      }else{
        new_code = codes(code_id)
      }
      for(idx <- (0 to 63)){
        map.+=(new_code(idx)-> std_code(idx))
      }
      map.+=('=' -> '=')
      CODE_TRANS_MAP.+=(code_id -> map)
      return src.map { x => map(x) }
    }
  }
}