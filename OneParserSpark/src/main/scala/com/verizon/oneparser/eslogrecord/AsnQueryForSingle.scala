package com.verizon.oneparser.eslogrecord

object AsnQueryForSingle {
  val dateTimePlaceHolder = "<DATETIME>" //
  val dateWithoutMills="dateWithoutMillis"
  val dateWithMills="dmTimeStamp"


  val query: String =
    "lteRrcConRel, lteRrcConSetupCmp, lteRrcConRel, pdschSum_0xB887,NRBeamFailureEvt, rachResult_0xb88a, rlcDlDataBytes, rlcRbConfigIndex, rlcDlNumRBs, rlcDlTimeStamp"

}

