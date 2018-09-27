package com.cpc.spark.common

import cpcutil.hash.MurmurHash3

/**
  * Created by roydong on 25/09/2018.
  */

object Murmur3Hash {
  def stringHash64(str: String, seed: Int): Long = {
    val d = str.toCharArray.map(_.toByte)
    val out = new MurmurHash3.LongPair
    MurmurHash3.murmurhash3_x64_128(d, 0, d.length, seed, out)
    out.val1
  }

  def stringHash128(str: String, seed: Int): (Long, Long) = {
    val d = str.toCharArray.map(_.toByte)
    val out = new MurmurHash3.LongPair
    MurmurHash3.murmurhash3_x64_128(d, 0, d.length, seed, out)
    (out.val1, out.val2)
  }
}
