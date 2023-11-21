package com.verizon.oneparser.schema

import java.sql.Timestamp

case class  LogRecord (fileName: String,
                       dmTimeStamp: Timestamp,
                       index: Int,
                       logCode: String,
                       hexLogCode: String,
                       version: Int,
                       insertedDate:String = "",
                       testId: Int,
                       exceptionOccured:Boolean = false,
                       exceptionCause:String="",
                       missingLogCode: Int = -1,
                       missingVersion:Int = -1,
                       missingScanMessage: String = "",
                       logRecordName:String = null,
                       logRecordASN: LogRecordASN=LogRecordASN(),
                       logRecordB192: LogRecordB192=LogRecordB192(),
                       logRecordB193: LogRecordB193=LogRecordB193(),
                       logRecordIP: LogRecordIP=LogRecordIP(),
                       logRecordNAS: LogRecordNAS=LogRecordNAS(),
                       logRecordQComm: LogRecordQComm=LogRecordQComm(),
                       logRecordQComm2: LogRecordQComm2=LogRecordQComm2(),
                       logRecordQComm5G: LogRecordQComm5G=LogRecordQComm5G(),
                       logRecordQComm5G2: LogRecordQComm5G2=LogRecordQComm5G2(),
                       logRecordSIG: LogRecordSIG=LogRecordSIG(),
                       logRecordTrace: LogRecordTrace=LogRecordTrace())
