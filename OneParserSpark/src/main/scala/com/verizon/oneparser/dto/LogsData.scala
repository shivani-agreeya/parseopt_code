package com.verizon.oneparser.dto

case class LogsData(fileName: String, id: Int, updatedDate: String, createdDate: String, fileLocation: String, DmUser: Int,
                    first_Name: String, last_Name: String, emailID: String, mdn_logs: String, imei_logs: String,
                    technology_logs: String, isinbuilding_stat: String, modelName_logs: String, hiveUpdatedDate: String, logFileSize: Long,
                    reportsHiveTblName: String, eventsCount: Int, hiveEndTime: String, startlogtime: String, lastlogtime: String, fileType: String, onDemandBy: Integer, reProcessedBy: Integer, carrierName: String, compressionStatus: Boolean, triggerCount: Integer)

