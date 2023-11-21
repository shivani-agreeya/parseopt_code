package com.verizon.oneparser

import com.verizon.oneparser.schema._


case class LogRecordWrapper (recordType:String = "",
                             logRecordObj: LogRecord_QComm = null,
                             logRecord2Obj:LogRecord_QComm2 = null,
                             logRecord_ASNObj: LogRecord_ASN = null,
                             logRecord_IPObj: LogRecord_IP = null,
                             logRecord_NASObj:LogRecord_NAS = null,
                             logRecord_B193Obj:LogRecord_B193 = null,
                             logRecord_B192Obj:LogRecord_B192 = null)

