package com.verizon.oneparser.broker

import com.typesafe.scalalogging.LazyLogging
import com.verizon.oneparser.common.Constants
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import com.verizon.oneparser.utils.dataframe.ExtraDataFrameOperations.implicits._

class KafkaUpdate(val producer: Producer) extends LazyLogging {
  def updateStatusInOldLogs(logsTblDs: Dataset[Row]): Unit = {

    // es_data_status should be "Under Processing" for those files which will be processed into dmat index
    var logsUpdated = logsTblDs
      .withColumn("esDataStatus", functions.when(functions.col("status") === Constants.HIVE_SUCCESS, Constants.UNDER_PROCESSING).otherwise(functions.col("status")))
      .withColumn("esEndTime", functions.lit(System.currentTimeMillis()))
    if (producer.commonConfigParameters.ODLV_TRIGGER) {
      logsUpdated = logsUpdated.withColumn("odlvOnpStatus", functions.col("status"))
    } else {
      logsUpdated = logsUpdated.withColumn("logfilestatusforlv", functions.lit(Constants.CONST_LV_READY))
    }
    val logs = fieldOrder(logsUpdated)
      .setNullableStateExcept("id")
    producer.send(logs, producer.commonConfigParameters.NOTIFICATION_KAFKA_TOPIC)
  }

  def updateEsStatusInLogs(logsTblDs:Dataset[Row], status:String): Unit ={
    if (logsTblDs != null) {
      var logsUpdated = logsTblDs
        .withColumn("esStartTime", functions.lit(System.currentTimeMillis()))
        .withColumn("status", functions.lit(status))
        .withColumn("esDataStatus", functions.lit(Constants.CONST_ANALYZING_DATA))
      if (producer.commonConfigParameters.ODLV_TRIGGER) logsUpdated = logsUpdated.withColumn("odlvOnpStatus", functions.lit(status))
      val logs = fieldOrder(logsUpdated)
          .setNullableStateExcept("id")
      producer.send(logs, producer.commonConfigParameters.NOTIFICATION_KAFKA_TOPIC)
    } else {
      logger.info("No Data Found while updating 'qcomm_status' for status:'"+status+"' into logs table:")
    }
  }

  def updateSeqInprogressStatusInLogs(logsTblDs: Dataset[Row], state: String, status: String): Unit = {
    if (logsTblDs != null) {
      val logs = fieldOrder(logsTblDs
          .withColumn("seqStartTime", functions.lit(System.currentTimeMillis()))
          .withColumn("status", functions.lit(status)))
          .setNullableStateExcept("id")
      producer.send(logs, producer.commonConfigParameters.NOTIFICATION_KAFKA_TOPIC)
    } else {
      logger.info("No Data Found while updating 'state' for status:'" + status + "' into logs table:")
    }
  }

  def fieldOrder(df: DataFrame) =
    df.select( "dmUser",  "creationTime",  "lastLogTime",  "fileName",  "size",  "mdn",  "imei",  "id",  "firstname",  "lastname",  "isinbuilding",  "hasgps",  "emailid",  "status",  "modelName",  "fileLocation",  "esDataStatus",  "esRecordCount",  "importstarttime",  "importstoptime",  "filetype",  "startLogTime",  "updatedTime",  "failureInfo",  "logfilestatusforlv",  "lvupdatedtime",  "lverrordetails",  "statusCsvFiles",  "csvUpdatedTime",  "csvErrorDetails",  "technology",  "seqStartTime",  "seqEndTime",  "hiveStartTime",  "hiveEndTime",  "esStartTime",  "esEndTime",  "lvfilestarttime",  "missingVersions",  "lvPick",  "lvStatus",  "lvparseduserid",  "lvlogfilecnt",  "processingServer",  "reportsHiveTblName",  "compressionstatus",  "eventsCount",  "ftpstatus",  "userPriorityTime",  "reprocessedBy",  "pcapMulti",  "pcapSingleRm",  "pcapSingleUm",  "cpHdfsTime", "cpHdfsStartTime", "cpHdfsEndTime",  "hiveDirEpoch",  "ondemandBy", "carrierName", "odlvUserId", "odlvUpload", "odlvOnpStatus", "odlvLvStatus", "zipStartTime", "zipEndTime", "logcodes", "triggerCount")

}
