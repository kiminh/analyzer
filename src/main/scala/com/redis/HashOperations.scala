package com.redis

import serialization._

trait HashOperations {
  self: Redis =>
  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>.
    * If <code>key</code> does not exist, a new key holding a hash is created.
    * If field already exists in the hash, it is overwritten.
    *
    * @see [[http://redis.io/commands/hset HSET documentation]]
    * @deprecated return value semantics is inconsistent with [[com.redis.HashOperations#hsetnx]] and
    *             [[com.redis.HashOperations#hmset]]. Use [[com.redis.HashOperations#hset1]] instead
    * @return <code>True</code> if <code>field</code> is a new field in the hash and value was set,
    *         <code>False</code> if <code>field</code> already exists in the hash and the value was updated.
    *
    */
  def hset(key: Any, field: Any, value: Any)(implicit format: Format): Boolean =
  send("HSET", List(key, field, value))(asBoolean)

  /** Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>.
    * If <code>key</code> does not exist, a new key holding a hash is created.
    * If field already exists in the hash, it is overwritten.
    *
    * @see [[http://redis.io/commands/hset HSET documentation]]
    * @return <code>Some(0)</code> if <code>field</code> is a new field in the hash and value was set,
    *         <code>Some(1)</code> if <code>field</code> already exists in the hash and the value was updated.
    */
  def hset1(key: Any, field: Any, value: Any)(implicit format: Format): Option[Long] =
  send("HSET", List(key, field, value))(asLong)

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>, only if field does not yet exist.
    * If key does not exist, a new key holding a hash is created.
    * If field already exists, this operation has no effect.
    *
    * @see [[http://redis.io/commands/hsetnx HSETNX documentation]]
    * @return <code>True</code> if <code>field</code> is a new field in the hash and value was set.
    *         </code>False</code> if <code>field</code> exists in the hash and no operation was performed.
    */
  def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean =
  send("HSETNX", List(key, field, value))(asBoolean)

  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("HGET", List(key, field))(asBulk)

  /**
    * Sets the specified fields to their respective values in the hash stored at key.
    * This command overwrites any existing fields in the hash.
    * If key does not exist, a new key holding a hash is created.
    *
    * @param map from fields to values
    * @see [[http://redis.io/commands/hmset HMSET documentation]]
    * @return <code>True</code> if operation completed successfully,
    *         <code>False</code> otherwise.
    */
  def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): Boolean =
  send("HMSET", key :: flattenPairs(map))(asBoolean)

  def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): Option[Map[K, V]] =
    send("HMGET", key :: fields.toList) {
      asList.map { values =>
        fields.zip(values).flatMap {
          case (field, Some(value)) => Some((field, value))
          case (_, None) => None
        }.toMap
      }
    }

  def hincrby(key: Any, field: Any, value: Long)(implicit format: Format): Option[Long] =
    send("HINCRBY", List(key, field, value))(asLong)

  def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): Option[Float] =
    send("HINCRBYFLOAT", List(key, field, value))(asBulk.map(_.toFloat))

  def hexists(key: Any, field: Any)(implicit format: Format): Boolean =
    send("HEXISTS", List(key, field))(asBoolean)

  def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Long] =
    send("HDEL", List(key, field) ::: fields.toList)(asLong)

  def hlen(key: Any)(implicit format: Format): Option[Long] =
    send("HLEN", List(key))(asLong)

  def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    send("HKEYS", List(key))(asList.map(_.flatten))

  def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    send("HVALS", List(key))(asList.map(_.flatten))

  @deprecated("Use the more idiomatic variant hgetall1, which has the returned Map behavior more consistent. See issue https://github.com/debasishg/scala-redis/issues/122", "3.2")
  def hgetall[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[Map[K, V]] =
    send("HGETALL", List(key))(asListPairs[K, V].map(_.flatten.toMap))

  def hgetall1[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[Map[K, V]] =
    send("HGETALL", List(key))(asListPairs[K, V].map(_.flatten.toMap)) match {
      case s@Some(m) if m.nonEmpty => s
      case _ => None
    }

  // HSCAN
  // Incrementally iterate hash fields and associated values (since 2.8)
  def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
  send("HSCAN", key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x) (if (count == 10) Nil else List("count", count)))(asPair)
}
