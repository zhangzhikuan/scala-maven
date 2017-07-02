package org.xuan

import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream}
import java.util

import redis.clients.jedis.Jedis


/**
  * Created by zhangzhikuan on 1/6/2017.
  */
object BV {
  def main(args: Array[String]): Unit = {
    val bytes = long2Byte(1028L)
    //val bis = new DataInputStream(new ByteArrayInputStream(bytes))
    //val vv = bis.readLong()
    //println(vv)

    println(Long.MaxValue)
    println((bytes(0) << 56).toLong)
    println(((bytes(1) & 255) << 48).toLong)
    println(((bytes(2) & 255) << 40).toLong)
    println(((bytes(3) & 255) << 32).toLong)
    println(((bytes(4) & 255) << 24).toLong)
    println(((bytes(5) & 255) << 16))
    println(((bytes(6) & 255) << 8).toLong)
    println(((bytes(7) & 255) << 0).toLong)
  }

  def long2Byte(v: Long): Array[Byte] = {
    val writeBuffer = new Array[Byte](8)
    writeBuffer(0) = (v >>> 56).toByte
    writeBuffer(1) = (v >>> 48).toByte
    writeBuffer(2) = (v >>> 40).toByte
    writeBuffer(3) = (v >>> 32).toByte
    writeBuffer(4) = (v >>> 24).toByte
    writeBuffer(5) = (v >>> 16).toByte
    writeBuffer(6) = (v >>> 8).toByte
    writeBuffer(7) = (v >>> 0).toByte
    writeBuffer
  }

  val redis = new Jedis("localhost");
  val all = new util.BitSet();
  val key: String = null
  val users = util.BitSet.valueOf(redis.get(key.getBytes()));
  all.or(users);

  all.cardinality()

}
