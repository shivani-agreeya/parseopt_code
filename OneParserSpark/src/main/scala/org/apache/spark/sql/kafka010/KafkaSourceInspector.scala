package org.apache.spark.sql.kafka010

/**
Contains code from the spark-sql-kafka-0-10_2.11-2.4.5 library used under the terms of the Apache 2.0 license:

  spark-sql-kafka-0-10_2.11-2.4.5 Library - https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.11/2.4.5
  Github repository: https://github.com/apache/spark

  Copyright 2020 The Apache Software Foundation

  http://www.apache.org/licenses/LICENSE-2.0.html

 */


import com.verizon.oneparser.broker.consumergroup.utils.ReflectionHelper
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanExec}
import org.apache.spark.sql.execution.{RDDScanExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.kafka010.{JsonUtils => KafkaJsonUtils}
import org.apache.spark.sql.sources.BaseRelation

import scala.collection.JavaConverters._

class KafkaSourceInspector(sparkPlan: SparkPlan) {
  def populateKafkaParams: Map[Int, Map[String, Object]] = {
    sparkPlan.collectLeaves().zipWithIndex.flatMap { case (plan, idx) =>
      val paramsOpt = plan match {
        case r: RowDataSourceScanExec if isKafkaRelation(r.relation) =>
          extractKafkaParamsFromKafkaRelation(r.relation)
        case r: RDDScanExec =>
          extractKafkaParamsFromDataSourceV1(r)
        case r: DataSourceV2ScanExec =>
          extractSourceTopicsFromDataSourceV2(r)
        case _ => None
      }
      if (paramsOpt.isDefined) {
        Some((idx, paramsOpt.get))
      } else {
        None
      }
    }.map(e => e._1 -> e._2).toMap
  }

  def partitionOffsets(str: String): Map[TopicPartition, Long] = KafkaJsonUtils.partitionOffsets(str)

  def extractSourceTopicsFromDataSourceV2(r: DataSourceV2ScanExec): Option[Map[String, Object]] =
    r.inputRDDs().flatMap { rdd =>
      rdd.partitions.flatMap {
        case e: DataSourceRDDPartition[_] => e.inputPartition match {
          case part: KafkaMicroBatchInputPartition =>
            Some(part.executorKafkaParams.asScala.toMap)
          case part: KafkaContinuousInputPartition =>
            Some(part.kafkaParams.asScala.toMap)
          case _ => None
        }
      }
    }.headOption


  def extractKafkaParamsFromDataSourceV1(r: RowDataSourceScanExec): Option[Map[String, Object]] = {
    extractKafkaParamsFromDataSourceV1(r.rdd)
  }

  private def isKafkaRelation(rel: BaseRelation): Boolean = rel match {
    case r: KafkaRelation => true
    case _ => false
  }

  private def extractKafkaParamsFromKafkaRelation(rel: BaseRelation): Option[Map[String, Object]] = {
    require(isKafkaRelation(rel))
    ReflectionHelper.reflectFieldWithContextClassloader[Map[String, String]](
      rel, "specifiedKafkaParams")
  }

  private def extractKafkaParamsFromDataSourceV1(r: RDDScanExec): Option[Map[String, Object]] = {
    extractKafkaParamsFromDataSourceV1(r.rdd)
  }

  private def extractKafkaParamsFromDataSourceV1(
                                                  rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    rddContainingPartition.partitions.flatMap {
      case _: KafkaSourceRDDPartition =>
        extractKafkaParamsFromKafkaSourceRDDPartition(rddContainingPartition)
      case _ => None
    }.headOption
  }

  private def extractKafkaParamsFromKafkaSourceRDDPartition(rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    def collectLeaves(rdd: RDD[_]): Seq[RDD[_]] = {
      if (rdd.dependencies.isEmpty) {
        Seq(rdd)
      } else {
        rdd.dependencies.map(_.rdd).flatMap(collectLeaves)
      }
    }

    collectLeaves(rddContainingPartition).flatMap {
      case r: KafkaSourceRDD => Some(r)
      case _ => None
    }.headOption.flatMap(extractKafkaParamsFromKafkaSourceRDD).orElse(None)
  }

  private def extractKafkaParamsFromKafkaSourceRDD(rdd: KafkaSourceRDD): Option[Map[String, Object]] =
    ReflectionHelper.reflectFieldWithContextClassloader[java.util.Map[String, Object]](
      rdd,
      "executorKafkaParams"
    ).map(_.asScala.toMap).orElse(None)
}
