package com.verizon.oneparser.broker.consumergroup.listener

import java.time.Duration

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.kafka010.KafkaSourceInspector
import org.apache.spark.sql.streaming.StreamingQueryListener
import com.verizon.oneparser.common.KafkaOffsetListenerConst


class KafkaOffsetListener extends StreamingQueryListener with Logging {

  import KafkaOffsetListenerConst._

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val query = SparkSession.active.streams.get(event.progress.id)
    if (query != null) {
      val exec = query match {
        case query: StreamingQueryWrapper => Some(query.streamingQuery.lastExecution)
        case query: StreamExecution => Some(query.lastExecution)
        case _ =>
          logWarning(s"Unexpected type of streaming query: ${query.getClass}")
          None
      }

      exec.filter(ex => ex != null).foreach { ex =>
        val inspector = new KafkaSourceInspector(ex.executedPlan)
        val idxToKafkaParams = inspector.populateKafkaParams
        idxToKafkaParams.foreach { case (idx, params) =>
          params.get(CONFIG_KEY_GROUP_ID) match {
            case Some(groupId) =>
              val sourceProgress = event.progress.sources(idx)
              val tpToOffsets = inspector.partitionOffsets(sourceProgress.endOffset)

              val newParams = new scala.collection.mutable.HashMap[String, Object]
              newParams ++= params
              newParams += "group.id" -> groupId

              val kafkaConsumer = new KafkaConsumer[String, String](newParams.asJava)
              try {
                val offsetsToCommit = tpToOffsets.map { case (tp, offset) =>
                  (tp -> new OffsetAndMetadata(offset))
                }
                kafkaConsumer.commitSync(offsetsToCommit.asJava, Duration.ofSeconds(10))
              } finally {
                kafkaConsumer.close()
              }

            case None =>
          }
        }
      }
    } else {
      logWarning(s"Cannot find query ${event.progress.id} from active spark session!")
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}
