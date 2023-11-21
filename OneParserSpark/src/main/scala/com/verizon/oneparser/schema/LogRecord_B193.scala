package com.verizon.oneparser.schema

import java.sql.Timestamp

case class LogRecord_B193(fileName: String,
                          dmTimeStamp: Timestamp,
                          index: Int,
                          logCode: String,
                          hexLogCode: String,
                          version: Int = 0,
                          insertedDate:String="",
                          testId: Int,
                          PhysicalCellID:List[Integer] =  null,
                          ServingCellIndex_B193:List[String] =  null,
                          IsServingCell:List[Integer] =  null,
                          CurrentSFN:List[Integer] =  null,
                          CurrentSubframeNumber:List[Integer] =  null,
                          IsRestricted:List[Integer] =  null,
                          CellTiming_0:List[Integer] =  null,
                          CellTiming_1:List[Integer] =  null,
                          CellTimingSFN_0:List[Integer] =  null,
                          CellTimingSFN_1:List[Integer] =  null,
                          InstRSRPRx_0:List[Double] =  null,
                          InstRSRPRx_1:List[Double] =  null,
                          InstRSRPRx_2:List[Double] =  null,
                          InstRSRPRx_3:List[Double] =  null,
                          InstMeasuredRSRP:List[Double] =  null,
                          InstRSRQRx_0:List[Double] =  null,
                          InstRSRQRx_1:List[Double] =  null,
                          InstRSRQRx_2:List[Double] =  null,
                          InstRSRQRx_3:List[Double] =  null,
                          InstRSRQ:List[Double] =  null,
                          InstRSSIRx_0:List[Double] =  null,
                          InstRSSIRx_1:List[Double] =  null,
                          InstRSSIRx_2:List[Double] =  null,
                          InstRSSIRx_3:List[Double] =  null,
                          InstRSSI:List[Double] =  null,
                          ResidualFrequencyError:List[Integer] =  null,
                          FTLSNRRx_0:List[Double] =  null,
                          FTLSNRRx_1:List[Double] =  null,
                          FTLSNRRx_2:List[Double] =  null,
                          FTLSNRRx_3:List[Double] =  null,
                          ProjectedSir:List[Long] =  null,
                          PostIcRsrq:List[Double] =  null,
                          cinr0:List[Integer] =  null,
                          cinr1:List[Integer] =  null,
                          cinr2:List[Integer] =  null,
                          cinr3:List[Integer] =  null,
                          NumberofSubPackets:Integer = null,
                          SubPacketID:List[Integer] = null,
                          SubPacketSize:List[Integer] = null,
                          EARFCN_B193:List[Integer] =  null,
                          NumofCell:List[Integer] =  null,
                          horxd_mode:List[Integer] =  null,
                          ValidRx:List[String] =  null,
                          FTLCumulativeFreqOffset:List[Integer] = null,
                          DuplexingMode:List[String] = null,
                          exceptionOccured:Boolean = false,
                          exceptionCause:String="",
                          missingVersion:Int = -1
                         )

