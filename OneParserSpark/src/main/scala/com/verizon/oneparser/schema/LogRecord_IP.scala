package com.verizon.oneparser.schema

import java.sql.Timestamp

case class LogRecord_IP(fileName: String,
                        dmTimeStamp: Timestamp,
                        index: Int,
                        logCode: String,
                        hexLogCode: String,
                        version: Int,
                        insertedDate:String = "",
                        testId: Int,
                        sipDirection: String = "",
                        sipCSeq: String = "",
                        sipMessageType: String = "",
                        sipByeReason: String = "",
                        sipMsg:String="",
                        voLTEDropEvt:Integer = null,
                        voLTEAbortEvt:Integer = null,
                        voLTETriggerEvt:Integer = null,
                        voLTECallEndEvt:Integer = null,
                        voLTECallNormalRelEvt:Integer = null,
                        rtpDirection: String = "",
                        rtpDlSn: java.lang.Long = null,
                        rtpRxTimeStamp: String = "",
                        exceptionOccured:Boolean = false,
                        exceptionCause:String="",
                        missingVersion:Int = -1
                       )
