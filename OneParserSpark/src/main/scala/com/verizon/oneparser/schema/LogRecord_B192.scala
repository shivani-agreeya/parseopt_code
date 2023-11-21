package com.verizon.oneparser.schema

import java.sql.Timestamp

case class LogRecord_B192(fileName: String,
                          dmTimeStamp: Timestamp,
                          index: Int,
                          logCode: String,
                          hexLogCode: String,
                          version: Int = 0,
                          insertedDate:String="",
                          testId: Int,
                          subPacketVersion:Int = 0,
                          EARFCN_B192:List[Integer] =  null,
                          DuplexingMode:List[String] = null,
                          PhysicalCellID:List[Integer] =  null,
                          FTLCumulativeFreqOffset:List[Integer] = null,
                          NumberofSubPackets:Integer = null,
                          InstRSRPRx_0:List[Double] =  null,
                          InstRSRPRx_1:List[Double] =  null,
                          InstMeasuredRSRP:List[Double] =  null,
                          InstRSRQRx_0:List[Double] =  null,
                          InstRSRQRx_1:List[Double] =  null,
                          InstRSRQ:List[Double] =  null,
                          InstRSSIRx_0:List[Double] =  null,
                          InstRSSIRx_1:List[Double] =  null,
                          InstRSSI:List[Double] =  null,
                          NumofCell:List[Integer] =  null,
                          SubPacketID:List[Integer] = null,
                          exceptionOccured:Boolean = false,
                          exceptionCause:String="",
                          missingVersion:Int = -1
                         )