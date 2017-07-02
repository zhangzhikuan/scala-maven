package org.xuan



import redis.clients.jedis.{BitOP, Jedis}

import scala.collection.BitSet
import scala.collection.JavaConverters._
/**
  * Created by zhangzhikuan on 1/6/2017.
  */
object BW {
  def main(args: Array[String]): Unit = {

    val redis = new Jedis("localhost", 6379)
    redis.select(0)
    val key="20170603_kuan"
    redis.setbit(key, 1023, true)
    val sets = java.util.BitSet.valueOf(redis.get(key.getBytes))

    println(sets.cardinality())
    for(i<- 0 until 1035){
      if(sets.get(i)){
        println(s"$i")
      }
    }

    redis.close()
  }
}
