package com.xuan

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaApusUtils, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhangzhikuan on 17-7-22.
  */
object App {
  def main(args: Array[String]): Unit = {
    val appName = "spark_Streaming_topic"
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10))

    val topics = Set("tp.kuan")
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "group.id" -> appName,
      "enable.auto.commit" -> "false"
    )
    val fromOffsets = KafkaApusUtils.getFromOffsets(kafkaParams, topics)

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
    val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc,
      kafkaParams, fromOffsets, messageHandler)

    ds.transform {
      rdd =>
        rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(println)
        rdd
    }.foreachRDD {
      rdd =>
        rdd.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
