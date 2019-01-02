package com.cpc.spark.common

import cpcutil.hash.MurmurHash3

/**
  * Created by roydong on 25/09/2018.
  */

object Murmur3Hash {

  def stringHash32(str: String, seed: Int): Int = {
    val d = str.getBytes()
    MurmurHash3.murmurhash3_x86_32(d, 0, d.length, seed)
  }

  def stringHash64(str: String, seed: Int): Long = {
    val d = str.getBytes()
    val out = new MurmurHash3.LongPair
    MurmurHash3.murmurhash3_x64_128(d, 0, d.length, seed, out)
    out.val1
  }

  def stringHash128(str: String, seed: Int): (Long, Long) = {
    val d = str.getBytes()
    val out = new MurmurHash3.LongPair
    MurmurHash3.murmurhash3_x64_128(d, 0, d.length, seed, out)
    (out.val1, out.val2)
  }
}
