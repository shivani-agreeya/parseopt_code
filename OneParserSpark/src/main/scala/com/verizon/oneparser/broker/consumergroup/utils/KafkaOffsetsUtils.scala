package com.verizon.oneparser.broker.consumergroup.utils

import java.util.{Properties, Map => JavaMap}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

object KafkaOffsetsUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def topicOffsets(bootstrapServers: String, groupId: String, topic: String, defaultOffsets: String): String = {
    val prop = new Properties()
    prop.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    val consumerGroupOffsetsResult = AdminClient.create(prop).listConsumerGroupOffsets(groupId)
    val tpOffsetsAndMetadata: JavaMap[TopicPartition, OffsetAndMetadata] = consumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get()
    val tpOffsets: Map[TopicPartition, Long] = tpOffsetsAndMetadata.asScala.mapValues(_.offset()).toMap
    val partitionOffsets: String = this.partitionOffsets(tpOffsets, topic)

    val topicOffsets = if(partitionOffsets.contains(topic)) partitionOffsets else defaultOffsets
    topicOffsets
  }

  private def partitionOffsets(partitionOffsets: Map[TopicPartition, Long], topic: String): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[TopicPartition] {
      override def compare(x: TopicPartition, y: TopicPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.filter(_.topic() == topic).toSeq.sorted  // sort for more determinism
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}
