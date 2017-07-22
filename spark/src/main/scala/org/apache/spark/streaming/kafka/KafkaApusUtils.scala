package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition

/**
  * Created by zhangzhikuan on 17-7-21.
  */
object KafkaApusUtils {
  def getFromOffsets(
                      kafkaParams: Map[String, String],
                      topics: Set[String]
                    ): Map[TopicAndPartition, Long] = {
    val kc = new KafkaCluster(kafkaParams)
    getFromOffsets(kc, kafkaParams, topics)
  }

  def getFromOffsets(
                      kc: KafkaCluster,
                      kafkaParams: Map[String, String],
                      topics: Set[String]
                    ): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }
}
